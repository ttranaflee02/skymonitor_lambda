import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, List
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
from minio import Minio
from minio.error import S3Error
from io import BytesIO
import sys
import os
import re

from src.common.config import Config
from src.utils.logging import setup_logger

logger = setup_logger(__name__, Config.LOG_LEVEL)

class FlightDataCrawler:
    """Web scraper for flight data from OpenSky Network website."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def fetch_from_opensky_website(self) -> Dict[str, Any]:
        """
        Crawl flight data from OpenSky Network live flight map.
        Falls back to REST API if web scraping fails.
        """
        try:
            # Try web scraping first
            flights = self._scrape_opensky_map()
            if flights:
                return {"states": flights, "time": int(datetime.utcnow().timestamp())}
        except Exception as e:
            logger.warning(f"Web scraping failed: {e}. Falling back to REST API...")
        
        # Fallback to REST API
        return self._fetch_from_rest_api()
    
    def _scrape_opensky_map(self) -> List[List]:
        """
        Scrape flight data from OpenSky live map using Selenium.
        Returns list of flight states in OpenSky format.
        """
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64)')
        
        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)
            logger.debug("Opening OpenSky live map...")
            driver.get('https://opensky-network.org/network/explorer')
            
            # Wait for flight table to load (max 15 seconds)
            wait = WebDriverWait(driver, 15)
            wait.until(EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, 'table tbody tr')
            ))
            
            # Extract JavaScript data from page
            page_source = driver.page_source
            flights = self._extract_flights_from_html(page_source)
            
            logger.info(f"✓ Scraped {len(flights)} flights from OpenSky website")
            return flights
        
        except Exception as e:
            logger.error(f"Web scraping error: {e}")
            return []
        finally:
            if driver:
                driver.quit()
    
    def _extract_flights_from_html(self, html: str) -> List[List]:
        """Extract flight data from OpenSky network explorer page."""
        flights = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find the flight table rows
            table_rows = soup.find_all('tr')
            
            for row in table_rows:
                cells = row.find_all('td')
                if len(cells) < 7:
                    continue
                
                try:
                    # Extract visible cell data
                    icao24 = cells[0].text.strip()
                    callsign = cells[1].text.strip() or None
                    origin_country = cells[2].text.strip()
                    
                    # Try to extract latitude/longitude from map data or data attributes
                    lat_text = cells[5].text.strip() if len(cells) > 5 else ""
                    lon_text = cells[4].text.strip() if len(cells) > 4 else ""
                    
                    latitude = float(re.search(r'-?\d+\.\d+', lat_text).group()) if re.search(r'-?\d+\.\d+', lat_text) else None
                    longitude = float(re.search(r'-?\d+\.\d+', lon_text).group()) if re.search(r'-?\d+\.\d+', lon_text) else None
                    
                    altitude_text = cells[6].text.strip() if len(cells) > 6 else "0"
                    altitude = float(re.search(r'\d+', altitude_text).group()) if re.search(r'\d+', altitude_text) else 0
                    
                    if icao24 and latitude and longitude:
                        # Format: OpenSky API state array format
                        flight_state = [
                            icao24,           # [0] icao24
                            callsign,         # [1] callsign
                            origin_country,   # [2] origin_country
                            int(datetime.utcnow().timestamp()),  # [3] time_position
                            int(datetime.utcnow().timestamp()),  # [4] last_contact
                            longitude,        # [5] longitude
                            latitude,         # [6] latitude
                            altitude,         # [7] baro_altitude
                            False,            # [8] on_ground
                            None,             # [9] velocity
                            None,             # [10] true_track
                            None,             # [11] vertical_rate
                            None,             # [12] sensors
                            altitude,         # [13] geo_altitude
                            None,             # [14] squawk
                            False,            # [15] spi
                            0,                # [16] position_source
                        ]
                        flights.append(flight_state)
                
                except (ValueError, IndexError, AttributeError) as e:
                    logger.debug(f"Error parsing row: {e}")
                    continue
            
            return flights
        
        except Exception as e:
            logger.error(f"HTML extraction error: {e}")
            return []
    
    def _fetch_from_rest_api(self) -> Dict[str, Any]:
        """Fetch from OpenSky REST API (most reliable method)."""
        try:
            # Public endpoint (no auth required)
            url = Config.OPENSKY_API_URL
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            logger.info(f"✓ Fetched {len(data.get('states', []))} flights from OpenSky REST API")
            return data
        
        except requests.Timeout:
            logger.error("OpenSky API timeout")
            return {"states": []}
        except requests.HTTPError as e:
            logger.error(f"OpenSky API HTTP error: {e.response.status_code}")
            return {"states": []}
        except requests.RequestException as e:
            logger.error(f"OpenSky API error: {e}")
            return {"states": []}


class OpenSkyFlightProducer:
    """
    Ingestion layer: crawls/fetches flight data, sends to Kafka (hot path),
    persists to MinIO (cold path) with year/month/day/hour partitioning.
    """
    
    def __init__(self):
        self.crawler = FlightDataCrawler()
        self.kafka_producer = None
        self.minio_client = None
        
        # Initialize with retries
        self._initialize_kafka()
        self._initialize_minio()
    
    def _initialize_kafka(self, retries: int = 5) -> None:
        """Initialize Kafka producer with retry logic."""
        for attempt in range(retries):
            try:
                brokers = [b.strip() for b in Config.KAFKA_BROKERS.split(",")]
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=brokers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=5,
                    request_timeout_ms=10000,
                    reconnect_backoff_ms=50,
                    reconnect_backoff_max_ms=1000
                )
                logger.info(f"✓ Kafka producer initialized: {Config.KAFKA_BROKERS}")
                return
            except (NoBrokersAvailable, KafkaError) as e:
                if attempt < retries - 1:
                    wait_time = (attempt + 1) * 5
                    logger.warning(f"Kafka unavailable (attempt {attempt + 1}/{retries}). Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to initialize Kafka after {retries} attempts: {e}")
                    raise
    
    def _initialize_minio(self, retries: int = 5) -> None:
        """Initialize MinIO client with retry logic."""
        for attempt in range(retries):
            try:
                self.minio_client = Minio(
                    Config.MINIO_ENDPOINT,
                    access_key=Config.MINIO_ROOT_USER,
                    secret_key=Config.MINIO_ROOT_PASSWORD,
                    secure=Config.MINIO_USE_SSL
                )
                
                if not self.minio_client.bucket_exists(Config.MINIO_BUCKET):
                    self.minio_client.make_bucket(Config.MINIO_BUCKET)
                    logger.info(f"✓ Created MinIO bucket: {Config.MINIO_BUCKET}")
                else:
                    logger.info(f"✓ MinIO bucket exists: {Config.MINIO_BUCKET}")
                return
            except Exception as e:
                if attempt < retries - 1:
                    wait_time = (attempt + 1) * 5
                    logger.warning(f"MinIO unavailable (attempt {attempt + 1}/{retries}). Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to initialize MinIO after {retries} attempts: {e}")
                    raise
    
    def fetch_flight_data(self) -> Dict[str, Any]:
        """Fetch flight data from website/API."""
        return self.crawler.fetch_from_opensky_website()
    
    def send_to_kafka(self, data: Dict[str, Any]) -> None:
        """Send flight data to Kafka topic."""
        if not data.get("states"):
            logger.debug("No flight states to send to Kafka")
            return
        
        if self.kafka_producer is None:
            logger.error("Kafka producer not initialized")
            return
        
        timestamp = datetime.utcnow().isoformat()
        sent_count = 0
        
        for flight_state in data["states"]:
            if not flight_state or len(flight_state) < 17:
                continue
            
            try:
                message = {
                    "timestamp": timestamp,
                    "icao24": flight_state[0],
                    "callsign": flight_state[1],
                    "origin_country": flight_state[2],
                    "time_position": flight_state[3],
                    "last_contact": flight_state[4],
                    "longitude": flight_state[5],
                    "latitude": flight_state[6],
                    "baro_altitude": flight_state[7],
                    "on_ground": flight_state[8],
                    "velocity": flight_state[9],
                    "true_track": flight_state[10],
                    "vertical_rate": flight_state[11],
                    "sensors": flight_state[12],
                    "geo_altitude": flight_state[13],
                    "squawk": flight_state[14],
                    "spi": flight_state[15],
                    "position_source": flight_state[16],
                }
                
                self.kafka_producer.send(Config.KAFKA_TOPIC_FLIGHTS, value=message)
                sent_count += 1
            
            except (KafkaError, Exception) as e:
                logger.error(f"Error queueing message to Kafka: {e}")
        
        try:
            self.kafka_producer.flush(timeout_ms=5000)
            logger.info(f"Sent {sent_count} flights to Kafka topic '{Config.KAFKA_TOPIC_FLIGHTS}'")
        except KafkaError as e:
            logger.error(f"Error flushing Kafka: {e}")
    
    def persist_to_minio(self, data: Dict[str, Any]) -> None:
        """Persist raw flight data to MinIO with partitioning."""
        if not data.get("states"):
            logger.debug("No flight states to persist to MinIO")
            return
        
        if self.minio_client is None:
            logger.error("MinIO client not initialized")
            return
        
        now = datetime.utcnow()
        object_name = f"{now.strftime('%Y/%m/%d/%H')}/flights_{now.strftime('%M%S')}.json"
        
        try:
            data_bytes = json.dumps(data).encode('utf-8')
            self.minio_client.put_object(
                Config.MINIO_BUCKET,
                object_name,
                BytesIO(data_bytes),
                length=len(data_bytes),
                content_type='application/json'
            )
            logger.info(f"Persisted {len(data['states'])} flights to MinIO: {object_name}")
        except S3Error as e:
            logger.error(f"MinIO S3Error: {e}")
        except Exception as e:
            logger.error(f"MinIO error: {e}")
    
    def run(self) -> None:
        """Main ingestion loop."""
        logger.info("=" * 60)
        logger.info("🚀 Starting Flight Data Producer (Web Crawler + API)")
        logger.info("=" * 60)
        logger.info(f"Configuration:")
        logger.info(f"  - Kafka brokers: {Config.KAFKA_BROKERS}")
        logger.info(f"  - Kafka topic: {Config.KAFKA_TOPIC_FLIGHTS}")
        logger.info(f"  - MinIO endpoint: {Config.MINIO_ENDPOINT}")
        logger.info(f"  - MinIO bucket: {Config.MINIO_BUCKET}")
        logger.info(f"  - Poll interval: {Config.POLL_INTERVAL_SECONDS}s")
        logger.info("=" * 60)
        
        poll_count = 0
        error_count = 0
        
        try:
            while True:
                poll_count += 1
                logger.debug(f"Poll #{poll_count}: Fetching flight data...")
                
                flight_data = self.fetch_flight_data()
                
                if flight_data.get("states"):
                    num_flights = len(flight_data["states"])
                    logger.info(f"✓ Fetched {num_flights} flights")
                    
                    try:
                        self.send_to_kafka(flight_data)
                    except Exception as e:
                        logger.error(f"Failed to send to Kafka: {e}")
                        error_count += 1
                    
                    try:
                        self.persist_to_minio(flight_data)
                    except Exception as e:
                        logger.error(f"Failed to persist to MinIO: {e}")
                        error_count += 1
                else:
                    logger.warning("No flights data returned")
                
                logger.debug(f"Sleeping for {Config.POLL_INTERVAL_SECONDS}s...")
                time.sleep(Config.POLL_INTERVAL_SECONDS)
        
        except KeyboardInterrupt:
            logger.info("\n" + "=" * 60)
            logger.info("⚠️  Shutting down producer (KeyboardInterrupt)...")
            logger.info("=" * 60)
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            error_count += 1
        finally:
            logger.info("=" * 60)
            logger.info(f"Producer Statistics:")
            logger.info(f"  - Total polls: {poll_count}")
            logger.info(f"  - Errors: {error_count}")
            logger.info("=" * 60)
            
            if self.kafka_producer:
                try:
                    self.kafka_producer.flush(timeout_ms=10000)
                    self.kafka_producer.close(timeout_secs=10)
                    logger.info("✓ Kafka producer closed")
                except Exception as e:
                    logger.error(f"Error closing Kafka: {e}")

def main():
    """Entry point."""
    try:
        producer = OpenSkyFlightProducer()
        producer.run()
    except Exception as e:
        logger.error(f"Failed to start producer: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()