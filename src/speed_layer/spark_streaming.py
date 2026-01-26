import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, to_timestamp, when, abs as spark_abs, current_timestamp
)

from src.common.config import Config
from src.speed_layer.schemas import FlightSchemas
from src.utils.logging import setup_logger

logger = setup_logger(__name__, Config.LOG_LEVEL)

class FlightStreamingProcessor:
    """
    Spark Structured Streaming:
    1. Read from Kafka
    2. Filter Vietnam airspace
    3. Detect rapid descent
    4. Write to MongoDB
    """
    
    def __init__(self):
        self.spark = self._init_spark()
    
    def _init_spark(self) -> SparkSession:
        spark = SparkSession.builder \
            .appName(Config.SPARK_APP_NAME_STREAMING) \
            .config("spark.mongodb.write.connection.uri", Config.MONGODB_URI) \
            .config("spark.mongodb.write.database", Config.MONGODB_DB) \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        spark.sparkContext.setLogLevel(Config.LOG_LEVEL)
        logger.info("Spark streaming session initialized")
        return spark
    
    def read_from_kafka(self) -> DataFrame:
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", Config.KAFKA_BROKERS) \
            .option("subscribe", Config.KAFKA_TOPIC_FLIGHTS) \
            .option("startingOffsets", "latest") \
            .load()
        
        df = df.select(
            from_json(col("value").cast("string"), FlightSchemas.FLIGHT_INPUT_SCHEMA).alias("flight")
        ).select("flight.*")
        
        return df
    
    def filter_vietnam_airspace(self, df: DataFrame) -> DataFrame:
        vietnam_filter = (
            (col("latitude") >= Config.VIETNAM_LAT_MIN) &
            (col("latitude") <= Config.VIETNAM_LAT_MAX) &
            (col("longitude") >= Config.VIETNAM_LON_MIN) &
            (col("longitude") <= Config.VIETNAM_LON_MAX)
        )
        return df.withColumn("in_vietnam", vietnam_filter)
    
    def detect_rapid_descent(self, df: DataFrame) -> DataFrame:
        df = df.withColumn("is_descending", col("vertical_rate") < 0)
        df = df.withColumn(
            "descent_rate",
            when(col("is_descending"), spark_abs(col("vertical_rate"))).otherwise(0.0)
        )
        df = df.withColumn(
            "descent_alert",
            (col("is_descending")) &
            (col("descent_rate") > abs(Config.RAPID_DESCENT_THRESHOLD)) &
            (col("baro_altitude") > Config.MIN_ALTITUDE_FOR_DESCENT) &
            (col("in_vietnam"))
        )
        return df
    
    def run(self) -> None:
        try:
            logger.info("Starting Spark Streaming pipeline...")
            
            df = self.read_from_kafka()
            df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
            df = self.filter_vietnam_airspace(df)
            df = self.detect_rapid_descent(df)
            
            df = df.select(
                col("timestamp"),
                col("icao24"),
                col("callsign"),
                col("origin_country"),
                col("longitude"),
                col("latitude"),
                col("baro_altitude").alias("altitude"),
                col("velocity"),
                col("vertical_rate"),
                col("in_vietnam"),
                col("is_descending"),
                col("descent_rate"),
                col("descent_alert"),
                current_timestamp().alias("processed_at")
            )
            
            query = df.writeStream \
                .format("mongodb") \
                .option("connection.uri", Config.MONGODB_URI) \
                .option("database", Config.MONGODB_DB) \
                .option("collection", Config.MONGODB_COLLECTION_FLIGHTS) \
                .option("checkpointLocation", "/tmp/mongodb_checkpoint") \
                .outputMode("append") \
                .trigger(processingTime=f"{Config.TRIGGER_INTERVAL_SECONDS}s") \
                .start()
            
            query.awaitTermination()
        
        except Exception as e:
            logger.error(f"Streaming error: {e}", exc_info=True)
            raise

def main():
    processor = FlightStreamingProcessor()
    processor.run()

if __name__ == "__main__":
    main()