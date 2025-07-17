import json
from datetime import datetime
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import MapStateDescriptor
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from prettytable import PrettyTable

SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000  # 7 days in ms


# ---- Timestamp Assigner Class ----
class EventTimeAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(value['event_time'])


# ---- Stateful RocksDB Processor ----
class RollingAggregator(KeyedProcessFunction):
    def open(self, context: RuntimeContext):
        descriptor = MapStateDescriptor(
            "event_state", Types.LONG(), Types.FLOAT()
        )
        self.event_map = context.get_map_state(descriptor)

    def process_element(self, value, ctx):
        user_id = value['user_id']
        txn_type = value['transaction_type']
        amount = float(value['transaction_amount'])
        event_time = value['event_time']

        # Add new event to RocksDB
        self.event_map.put(event_time, amount)

        # Remove stale events older than 7 days
        cutoff = event_time - SEVEN_DAYS_MS
        stale_keys = [k for k in self.event_map.keys() if k < cutoff]
        for k in stale_keys:
            self.event_map.remove(k)

        # Compute total
        total = sum(self.event_map.values())

        # Print with PrettyTable
        table = PrettyTable()
        table.field_names = ["User ID", "Type", "7d Total"]
        table.add_row([user_id, txn_type, f"{total:.2f}"])
        print(table)


# ---- Main Job ----
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # ---- RocksDB backend ----
    from pyflink.java_gateway import get_gateway
    j_backend = get_gateway().jvm.org.apache.flink.contrib.streaming.state.RocksDBStateBackend("file:///tmp/flink-rocksdb", True)
    env.set_state_backend(j_backend)

    # ---- Kafka consumer ----
    kafka_props = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "txn-group",
        "auto.offset.reset": "latest"
    }

    consumer = FlinkKafkaConsumer(
        topics='transactions',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    raw_stream = env.add_source(consumer)

    # ---- Parse JSON and attach event time ----
    def parse_json(record):
        obj = json.loads(record)
        obj['event_time'] = int(datetime.strptime(obj['timestamp_str'], "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        return obj

    parsed = raw_stream.map(parse_json, output_type=Types.MAP(Types.STRING(), Types.STRING()))

    # ---- Assign Watermarks ----
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(EventTimeAssigner())
    with_timestamps = parsed.assign_timestamps_and_watermarks(watermark_strategy)

    # ---- Key by (user_id, transaction_type) ----
    keyed = with_timestamps.key_by(lambda x: (x['user_id'], x['transaction_type']))

    # ---- Stateful aggregation ----
    keyed.process(RollingAggregator())

    # ---- Run the job ----
    env.execute("7-day Rolling Aggregation with RocksDB")


if __name__ == "__main__":
    main()
