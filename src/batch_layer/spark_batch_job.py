import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_date, from_unixtime, count, countDistinct,
    explode, hour
)

from src.common.config import Config
from src.utils.logging import setup_logger

logger = setup_logger(__name__, Config.LOG_LEVEL)

class FlightBatchAnalytics:
    """
    Batch layer: reads MinIO, aggregates flight statistics,
    writes to Elasticsearch.
    """
    
    def __init__(self):
        self.spark = self._init_spark()
    
    def _init_spark(self) -> SparkSession:
        spark = SparkSession.builder \
            .appName(Config.SPARK_APP_NAME_BATCH) \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()
        spark.sparkContext.setLogLevel(Config.LOG_LEVEL)
        logger.info("Spark batch session initialized")
        return spark
    
    def read_raw_data_from_minio(self, date_str: str) -> DataFrame:
        """Read raw data from MinIO for given date (YYYY-MM-DD)."""
        parts = date_str.split("-")
        year, month, day = parts[0], parts[1], parts[2]
        s3_path = f"s3a://{Config.MINIO_BUCKET}/{year}/{month}/{day}/*/*.json"
        
        logger.info(f"Reading from: {s3_path}")
        
        try:
            df = self.spark.read.option("multiline", "true").json(s3_path)
            logger.info(f"Loaded {df.count()} records")
            return df
        except Exception as e:
            logger.error(f"Error reading MinIO: {e}")
            return self.spark.createDataFrame([], self.spark.sparkContext.parallelize([]).toDF().schema)
    
    def flatten_flight_states(self, df: DataFrame) -> DataFrame:
        """Flatten states array from raw API response."""
        if "states" not in df.columns:
            logger.warning("No 'states' column found")
            return df
        
        df = df.select(
            col("time").alias("timestamp_unix"),
            explode(col("states")).alias("state")
        )
        
        df = df.select(
            from_unixtime(col("timestamp_unix")).alias("timestamp"),
            col("state")[0].alias("icao24"),
            col("state")[1].alias("callsign"),
            col("state")[2].alias("origin_country"),
            col("state")[5].alias("longitude"),
            col("state")[6].alias("latitude"),
            col("state")[7].alias("altitude"),
        ).filter(col("icao24").isNotNull())
        
        logger.debug(f"Flattened to {df.count()} records")
        return df
    
    def aggregate_by_airline(self, df: DataFrame) -> DataFrame:
        """Aggregate flights by origin country."""
        agg_df = df.groupBy("origin_country") \
            .agg(
                countDistinct("icao24").alias("unique_flights"),
                count("*").alias("total_positions")
            ) \
            .withColumn("analysis_date", to_date(col("timestamp"))) \
            .select(
                col("analysis_date"),
                col("origin_country"),
                col("unique_flights"),
                col("total_positions")
            )
        
        logger.info(f"Airline aggregation: {agg_df.count()} countries")
        return agg_df
    
    def aggregate_density_by_hour(self, df: DataFrame) -> DataFrame:
        """Calculate flight density per hour."""
        density_df = df.withColumn("hour", hour(col("timestamp"))) \
            .groupBy(
                to_date(col("timestamp")).alias("analysis_date"),
                col("hour")
            ) \
            .agg(
                count("*").alias("flight_count"),
                countDistinct("icao24").alias("unique_aircraft")
            ) \
            .withColumn("avg_density", col("flight_count") / 3600.0)
        
        logger.info(f"Hourly density: {density_df.count()} hours")
        return density_df
    
    def write_to_elasticsearch(self, df: DataFrame, index_suffix: str) -> None:
        """Write aggregated data to Elasticsearch."""
        es_url = f"http://{Config.ELASTICSEARCH_HOST}:{Config.ELASTICSEARCH_PORT}"
        index_name = f"{Config.ELASTICSEARCH_INDEX}-{index_suffix}"
        
        try:
            df.write \
                .format("org.elasticsearch.spark.sql") \
                .option("es.nodes", Config.ELASTICSEARCH_HOST) \
                .option("es.port", Config.ELASTICSEARCH_PORT) \
                .option("es.resource", index_name) \
                .option("es.input.json", "false") \
                .mode("append") \
                .save()
            logger.info(f"Wrote to Elasticsearch: {index_name}")
        except Exception as e:
            logger.error(f"Error writing to Elasticsearch: {e}")
    
    def run(self, date_str: str = None) -> None:
        """Execute batch job for given date (default: yesterday)."""
        if date_str is None:
            date_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info(f"Starting batch analytics for {date_str}")
        
        try:
            # Read and flatten
            raw_df = self.read_raw_data_from_minio(date_str)
            if raw_df.count() == 0:
                logger.warning(f"No data found for {date_str}")
                return
            
            flights_df = self.flatten_flight_states(raw_df)
            
            # Aggregate
            airline_agg = self.aggregate_by_airline(flights_df)
            density_agg = self.aggregate_density_by_hour(flights_df)
            
            # Write
            self.write_to_elasticsearch(airline_agg, "airlines")
            self.write_to_elasticsearch(density_agg, "density")
            
            logger.info(f"Batch job completed for {date_str}")
        
        except Exception as e:
            logger.error(f"Batch job error: {e}", exc_info=True)
            raise

def main():
    job = FlightBatchAnalytics()
    job.run()

if __name__ == "__main__":
    main()