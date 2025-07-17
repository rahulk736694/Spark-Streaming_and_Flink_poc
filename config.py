# config.py

# HDFS paths
HDFS_RAW_TXN_PATH = "hdfs://localhost:9000/flink/raw_transactions_test/"
HDFS_KI_OUTPUT_PATH = "hdfs://localhost:9000/flink/KI_Indicators/"

# Kafka settings
KAFKA_BROKERS = "localhost:9092"
KAFKA_TOPIC = "transactions-topic"
KAFKA_GROUP_ID = "flink-after-spark-consumer"

# Checkpointing
CHECKPOINT_INTERVAL_MS = 60000

# Watermark settings
WATERMARK_DELAY_MINUTES = 5

# State TTL in days
STATE_TTL_DAYS = 2

# Aggregation window
ROLLING_WINDOW_DAYS = 2

# Transaction metadata
TRANSACTION_TYPES = ['atm', 'check']
DIRECTION_TYPES = ['dep', 'credit']
