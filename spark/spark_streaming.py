import os
import time
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
)
from pyspark.sql.functions import (
    from_json, col, lit, current_timestamp,
    sum as _sum, avg, max as _max, min as _min, unix_timestamp
)
from config_spark import *

#Logging 
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--conf spark.driver.extraJavaOptions=-Dlog4j.configurationFile={LOG4J_CONFIG_PATH} pyspark-shell"

# Schemas
KAFKA_SCHEMA = StructType([
    StructField("transaction_amount", StringType(), True),
    StructField("timestamp_str", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("dep_or_withdraw", StringType(), True)
])

HISTORICAL_SCHEMA = StructType([
    StructField("transaction_amount", DecimalType(18, 2), True),
    StructField("timestamp_str", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("transaction_id", IntegerType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("dep_or_withdraw", StringType(), True),
    StructField("event_timestamp", TimestampType(), True)
])

# Spark Session
spark = SparkSession.builder \
    .appName(SPARK_APP_NAME) \
    .master(SPARK_MASTER) \
    .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY) \
    .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
    .config("spark.executor.cores", SPARK_EXECUTOR_CORES) \
    .config("spark.jars", JARS_PATH) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load Historical Data 
print("Loading historical data from HDFS...")
try:
    raw_df = spark.read \
        .option("header", "true") \
        .schema(HISTORICAL_SCHEMA) \
        .csv(HDFS_RAW_TXN_PATH) \
        .withColumn("event_timestamp", col("timestamp_str").cast(TimestampType()))
    historical_df = raw_df.cache()
    print(f"Historical records: {historical_df.count()}")
except Exception as e:
    print(f"Failed to load historical data. Proceeding with empty DataFrame. Reason: {e}")
    historical_df = spark.createDataFrame([], HISTORICAL_SCHEMA)

# Reading from Kafka
print("Reading live transactions from Kafka...")
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), KAFKA_SCHEMA).alias("data")) \
    .select(
        col("data.transaction_amount").cast(DecimalType(18, 2)).alias("transaction_amount"),
        col("data.timestamp_str").alias("timestamp_str"),
        col("data.user_id").cast(IntegerType()).alias("user_id"),
        col("data.transaction_id").cast(IntegerType()).alias("transaction_id"),
        col("data.transaction_type").alias("transaction_type"),
        col("data.dep_or_withdraw").alias("dep_or_withdraw")
    ) \
    .withColumn("event_timestamp", col("timestamp_str").cast(TimestampType())) \
    .withWatermark("event_timestamp", "10 minutes")

# Processing Part
def process_batch(batch_df, batch_id):
    print(f"\nProcessing micro-batch {batch_id}...")
    start_time = time.time()
    try:
        if batch_df.rdd.isEmpty():
            print("Batch is empty. Skipping...")
            return
        
        latency_df = batch_df.withColumn("processing_time", current_timestamp()) \
            .withColumn("latency_sec", (unix_timestamp("processing_time") - unix_timestamp("event_timestamp")).cast("double")) \
            .select("transaction_id", "user_id", "latency_sec")

        for row in latency_df.collect():
            print(f"[LATENCY] txn_id={row['transaction_id']} user={row['user_id']} latency={row['latency_sec']:.3f} sec")

        
        raw_output_path = os.path.join(HDFS_RAW_TXN_PATH, f"dt={datetime.now().strftime('%Y-%m-%d')}")
        batch_df.write.mode("append").option("header", "true").csv(raw_output_path)
        print(f"Appended raw transactions to HDFS: {raw_output_path}")

        now = datetime.now(timezone.utc)
        window_start = now - DEFAULT_WINDOW_DELTA

        hist_filtered = historical_df.filter(
            (col("event_timestamp") >= lit(window_start)) &
            (col("event_timestamp") <= lit(now))
        ).select(batch_df.columns)

        combined_df = hist_filtered.unionByName(batch_df)

        filtered_df = combined_df.filter(
            (col("event_timestamp") >= lit(window_start)) &
            (col("event_timestamp") <= lit(now))
        )

        if filtered_df.rdd.isEmpty():
            print("No data in 2-day window. Skipping batch.")
            return

        base_df = filtered_df.select("user_id").distinct() \
            .withColumn("window_start", lit(window_start)) \
            .withColumn("window_end", lit(now)) \
            .withColumn("processing_time", current_timestamp())

        agg_df = base_df
        for tx_type in TRANSACTION_TYPES:
            for direction in DIRECTION_TYPES:
                metrics_df = filtered_df.filter(
                    (col("transaction_type") == tx_type) &
                    (col("dep_or_withdraw") == direction)
                ).groupBy("user_id").agg(
                    _sum("transaction_amount").alias(f"last_2d_tot_{tx_type}_{direction}"),
                    avg("transaction_amount").alias(f"last_2d_avg_{tx_type}_{direction}"),
                    _max("transaction_amount").alias(f"last_2d_max_{tx_type}_{direction}"),
                    _min("transaction_amount").alias(f"last_2d_min_{tx_type}_{direction}")
                )
                agg_df = agg_df.join(metrics_df, on="user_id", how="left")

        for tx_type in TRANSACTION_TYPES:
            for direction in DIRECTION_TYPES:
                for metric in ["tot", "avg", "max", "min"]:
                    col_name = f"last_2d_{metric}_{tx_type}_{direction}"
                    agg_df = agg_df.fillna({col_name: 0}).withColumn(col_name, col(col_name).cast(DecimalType(18, 2)))

        output_path = os.path.join(HDFS_KI_OUTPUT_PATH, f"dt={now.strftime('%Y-%m-%d')}")
        agg_df.write.mode("append").option("header", "true").csv(output_path)
        print(f"Saved user aggregates to HDFS: {output_path}")

      
        end_time = time.time()
        duration = end_time - start_time + 1e-6
        msg_count = batch_df.count()
        print(f"Processed {msg_count} messages in {duration:.3f} sec → Throughput: {msg_count/duration:.2f} msg/sec")

    except Exception as e:
        print(f"Error in batch {batch_id}: {e}")

# Start Streaming Point
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .trigger(processingTime='1 seconds') \
    .start()

print("Spark Streaming job started. Waiting for termination...")
query.awaitTermination()
