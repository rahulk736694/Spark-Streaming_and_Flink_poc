
import os
from datetime import timedelta

SPARK_APP_NAME = "SparkKafkaHDFSUserAggregates"
SPARK_MASTER = "local[*]"

SPARK_EXECUTOR_MEMORY = "2048m"
SPARK_DRIVER_MEMORY = "1600m"
SPARK_EXECUTOR_CORES = "4"

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
JARS_PATH = ",".join([
    os.path.join(BASE_DIR, "Bin", "spark-sql-kafka-0-10_2.12-3.4.1.jar"),
    os.path.join(BASE_DIR, "Bin", "spark-token-provider-kafka-0-10_2.12-3.4.1.jar"),
    os.path.join(BASE_DIR, "Bin", "kafka-clients-2.8.1.jar"),
    os.path.join(BASE_DIR, "Bin", "hadoop-client-3.3.4.jar"),
    os.path.join(BASE_DIR, "Bin", "commons-pool2-2.11.1.jar")
])

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transactions-topic"

HDFS_RAW_TXN_PATH = "hdfs://localhost:9000/spark/raw_transaction/"
HDFS_KI_OUTPUT_PATH = "hdfs://localhost:9000/spark/key_indicator/"
CHECKPOINT_DIR = "/tmp/spark/kafka_checkpoint"

TRANSACTION_TYPES = ["atm", "check"]
DIRECTION_TYPES = ["dep", "credit"]

DEFAULT_WINDOW_DAYS = 2
DEFAULT_WINDOW_DELTA = timedelta(days=DEFAULT_WINDOW_DAYS)

LOG4J_CONFIG_PATH = "/home/rahul/Code/practice/log4j2.xml"
