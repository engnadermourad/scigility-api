import os
import threading
import json
from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType

router = APIRouter()

spark_session = None
spark_thread = None

# Paths for streaming output and checkpointing
STREAM_OUTPUT_PATH = "/tmp/stream_data"
STREAM_CHECKPOINT_PATH = "/tmp/stream_checkpoint"

def stream_job():
    global spark_session

    spark_session = SparkSession.builder \
        .appName("BTC-USD-Stream-Consumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

    spark_session.sparkContext.setLogLevel("ERROR")

    # Get Kafka config from environment
    KAFKA_SERVER = os.getenv("KAFKA_SERVER", "broker_1:29094")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_NAME", "btc-usdt-realtime")

    # Define schema for incoming Kafka messages
    schema = StructType() \
        .add("timestamp", TimestampType()) \
        .add("symbol", StringType()) \
        .add("price", DoubleType())

    # Read stream from Kafka
    df_kafka = spark_session.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse and transform the Kafka message
    df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")

    df_json = df_parsed.select(to_json(struct("*")).alias("value"))

    # Write to JSON files
    query = df_json.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", STREAM_OUTPUT_PATH) \
        .option("checkpointLocation", STREAM_CHECKPOINT_PATH) \
        .start()

    query.awaitTermination()

@router.post("/start")
def start_stream():
    global spark_thread
    if spark_thread and spark_thread.is_alive():
        return {"status": "already running"}

    spark_thread = threading.Thread(target=stream_job, daemon=True)
    spark_thread.start()
    return {"status": "streaming started"}

@router.post("/stop")
def stop_stream():
    global spark_session
    if spark_session:
        spark_session.stop()
        spark_session = None
        return {"status": "stopped"}
    return {"status": "not running"}

@router.get("/latest")
def get_latest_stream_data(limit: int = 100000000):
    files = sorted(Path(STREAM_OUTPUT_PATH).glob("*.json"), reverse=True)
    data = []

    for file in files:
        try:
            with open(file, "r") as f:
                for line in f:
                    if line.strip():
                        data.append(json.loads(line))
        except Exception:
            continue

        if len(data) >= limit:
            break

    data = sorted(data, key=lambda x: x.get("timestamp", ""), reverse=True)
    return JSONResponse(content=data[:limit])
