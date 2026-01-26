
import json
import time
import logging
from datetime import datetime, timezone
from typing import List, Optional
import requests
from requests.auth import HTTPBasicAuth

# New Imports
import sys
sys.path.append("/Users/tranlephuongthao/Documents/MSc_Data_Science/big data/skymonitor_lambda/")
from src.common.config import Config
from src.common.models import FlightState
from src.utils.logging import setup_logger
from src.utils.minio_client import MinIOHelper

logger = setup_logger(__name__, Config.LOG_LEVEL)

class OpenSkyFlightProducer:
    """
    Ingestion layer: polls OpenSky API via direct REST calls, sends to Kafka (hot path),
    persists to MinIO (cold path) with year/month/day/hour partitioning.
    """
    
    def __init__(self):
        self.kafka_producer = self._init_kafka()
        self.minio_helper = MinIOHelper(Config)
        
        # REST API coordinates
        self.api_url = Config.OPENSKY_API_URL
        self.auth = None
        if Config.OPENSKY_USERNAME and Config.OPENSKY_PASSWORD:
            self.auth = HTTPBasicAuth(Config.OPENSKY_USERNAME, Config.OPENSKY_PASSWORD)
            logger.info("Using authenticated OpenSky REST API access")
        else:
            logger.warning("No OpenSky credentials found, using anonymous access (heavy rate limits)")

    def _init_kafka(self):
        from kafka import KafkaProducer
        try:
            producer = KafkaProducer(
                bootstrap_servers=Config.KAFKA_BROKERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info(f"Kafka producer initialized: {Config.KAFKA_BROKERS}")
            return producer
        except Exception as e:
            logger.error(f"Failed to initialize Kafka: {e}")
            raise
    
    def fetch_flight_data(self) -> Optional[List[FlightState]]:
        """Fetch states from OpenSky REST API and convert to a list of FlightState objects."""
        try:
            response = requests.get(self.api_url, auth=self.auth, timeout=30)
            
            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After', 'unknown')
                logger.error(f"OpenSky API Rate Limit hit (429). Retry-After: {retry_after}")
                return None
                
            response.raise_for_status()
            data = response.json()
            
            states = data.get('states')
            if not states:
                return None
            
            timestamp = datetime.now(timezone.utc).isoformat()
            flight_states = []
            
            for s in states:
                # OpenSky REST API response fields map to FlightState
                state = FlightState(
                    timestamp=timestamp,
                    icao24=s[0],
                    callsign=s[1].strip() if s[1] else None,
                    origin_country=s[2],
                    time_position=s[3],
                    last_contact=s[4],
                    longitude=s[5],
                    latitude=s[6],
                    baro_altitude=s[7],
                    on_ground=s[8],
                    velocity=s[9],
                    true_track=s[10],
                    vertical_rate=s[11],
                    sensors=s[12],
                    geo_altitude=s[13],
                    squawk=s[14],
                    spi=s[15],
                    position_source=s[16],
                )
                flight_states.append(state)
            return flight_states
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error fetching data: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching data: {e}")
            return None
    
    def send_to_kafka(self, messages: List[FlightState]) -> None:
        if not messages:
            return
        
        for message in messages:
            try:
                self.kafka_producer.send(Config.KAFKA_TOPIC_FLIGHTS, value=message.to_dict())
            except Exception as e:
                logger.error(f"Error sending to Kafka: {e}")
    
    def persist_to_minio(self, messages: List[FlightState]) -> None:
        if not messages:
            return
        
        now = datetime.now(timezone.utc)
        object_name = f"{now.strftime('%Y/%m/%d/%H')}/flights_{now.strftime('%M%S')}.json"
        
        try:
            data = {"states": [msg.to_dict() for msg in messages]}
            self.minio_helper.upload_json(object_name, data)
            logger.debug(f"Persisted {len(messages)} flights to {object_name}")
        except Exception as e:
            logger.error(f"MinIO error: {e}")
    
    def run(self) -> None:
        logger.info("Starting OpenSky producer (pyopensky mode)...")
        try:
            while True:
                messages = self.fetch_flight_data()
                if messages:
                    logger.info(f"Fetched {len(messages)} flights")
                    self.send_to_kafka(messages)
                    self.persist_to_minio(messages)
                else:
                    logger.warning("No flight data returned or error occurred.")
                
                time.sleep(Config.POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("Producer shutdown")
        finally:
            self.kafka_producer.flush()
            self.kafka_producer.close()

def main():
    producer = OpenSkyFlightProducer()
    producer.run()

if __name__ == "__main__":
    main()