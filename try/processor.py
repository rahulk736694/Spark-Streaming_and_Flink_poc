
# from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from pyflink.datastream.state import ListStateDescriptor
# from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
# from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.common.typeinfo import Types
# from pyflink.common import Row, Time
# from pyflink.datastream.window import SlidingEventTimeWindows
# from pyflink.table import StreamTableEnvironment
# from datetime import datetime
# import json

# # === RocksDB Union Processor ===
# class EmitUnionedTransactions(KeyedProcessFunction):

#     def open(self, runtime_context: RuntimeContext):
#         self.yesterday_txns = runtime_context.get_list_state(
#             ListStateDescriptor("yesterday_txns", Types.STRING()))
#         self.today_txns = runtime_context.get_list_state(
#             ListStateDescriptor("today_txns", Types.STRING()))

#     def process_element(self, value: str, ctx):
#         self.today_txns.add(value)

#         combined = list(self.yesterday_txns.get()) + list(self.today_txns.get())

#         print("=== UNIONED TRANSACTIONS DEBUG START ===")
#         for txn_json in combined:
#             try:
#                 txn = json.loads(txn_json)
#                 print(f"[DEBUG] txn: {txn}")

#                 ts = datetime.strptime(txn["timestamp_str"], "%Y-%m-%d %H:%M:%S")

#                 yield Row(
#                     int(txn["user_id"]),
#                     float(txn["transaction_amount"]),
#                     txn["dep_or_withdraw"],
#                     txn["timestamp_str"],
#                     int(txn["transaction_id"]),
#                     txn["transaction_type"],
#                     ts
#                 )

#             except Exception as e:
#                 print(f"[ERROR] Failed to emit transaction: {e}")
#         print("=== UNIONED TRANSACTIONS DEBUG END ===")

# # === Main Flink Job ===
# def main():
#     # --- Set up environment ---
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)
#     env.set_state_backend(EmbeddedRocksDBStateBackend())
#     env.enable_checkpointing(60000)
#     env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

#     t_env = StreamTableEnvironment.create(env)

#     # --- Kafka source ---
#     kafka_props = {
#         'bootstrap.servers': 'localhost:9092',
#         'group.id': 'rocksdb-union-ki'
#     }

#     kafka_source = FlinkKafkaConsumer(
#         topics='transactions-topic',
#         deserialization_schema=SimpleStringSchema(),
#         properties=kafka_props
#     )

#     stream = env.add_source(kafka_source)

#     # --- RocksDB1 + RocksDB2 Union ---
#     unioned = stream \
#         .key_by(lambda x: json.loads(x)['user_id'], key_type=Types.INT()) \
#         .process(EmitUnionedTransactions(),
#                  output_type=Types.ROW_NAMED(
#                      ['user_id', 'transaction_amount', 'dep_or_withdraw',
#                       'timestamp_str', 'transaction_id', 'transaction_type', 'event_time'],
#                      [Types.INT(), Types.FLOAT(), Types.STRING(),
#                       Types.STRING(), Types.INT(), Types.STRING(), Types.SQL_TIMESTAMP()])
#                  )

#     # --- Register Table View for Raw Unioned Transactions ---
#     t_env.create_temporary_view("unioned_raw_txns", unioned)

#     # --- Sink Raw Unioned Data to HDFS ---
#     t_env.execute_sql("""
#         CREATE TABLE hdfs_raw_union (
#             user_id INT,
#             transaction_amount FLOAT,
#             dep_or_withdraw STRING,
#             timestamp_str STRING,
#             transaction_id INT,
#             transaction_type STRING,
#             event_time TIMESTAMP(3)
#         ) WITH (
#             'connector' = 'filesystem',
#             'path' = 'hdfs://localhost:9000/flink/raw_union_txns/',
#             'format' = 'csv'
#         )
#     """)

#     t_env.execute_sql("""
#         INSERT INTO hdfs_raw_union
#         SELECT * FROM unioned_raw_txns
#     """)

#     # --- 7-day Sliding Window Aggregation ---
#     windowed = unioned \
#         .key_by(lambda row: row.user_id, key_type=Types.INT()) \
#         .window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1))) \
#         .reduce(
#             lambda a, b: Row(
#                 a.user_id,
#                 a.transaction_amount + b.transaction_amount,
#                 a.event_time
#             ),
#             output_type=Types.ROW_NAMED(
#                 ['user_id', 'total_amount', 'event_time'],
#                 [Types.INT(), Types.FLOAT(), Types.SQL_TIMESTAMP()])
#         )

#     # --- Sink Aggregated KIs to HDFS ---
#     t_env.create_temporary_view("sliding_agg_result", windowed)

#     t_env.execute_sql("""
#         CREATE TABLE hdfs_sliding_ki (
#             user_id INT,
#             total_amount FLOAT,
#             event_time TIMESTAMP(3)
#         ) WITH (
#             'connector' = 'filesystem',
#             'path' = 'hdfs://localhost:9000/flink/sliding_key_indicators/',
#             'format' = 'csv'
#         )
#     """)

#     t_env.execute_sql("""
#         INSERT INTO hdfs_sliding_ki
#         SELECT * FROM sliding_agg_result
#     """)

#     # --- Launch Job ---
#     env.execute("RocksDB Union to HDFS Raw + Aggregated")

# if __name__ == "__main__":
#     main()

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor
from pyflink.common import Types, Row
from collections import defaultdict
import json
from datetime import datetime, timedelta
from config import TRANSACTION_TYPES, DIRECTION_TYPES, logger


class HistoricalLoader(KeyedProcessFunction):
    def open(self, ctx: RuntimeContext):
        self.historical = ctx.get_list_state(
            ListStateDescriptor("historical_txns", Types.STRING())
        )

    def process_element(self, value, ctx):
        self.historical.add(value.raw_json)


class RealtimeAggregator(KeyedProcessFunction):
    def open(self, ctx: RuntimeContext):
        self.historical = ctx.get_list_state(
            ListStateDescriptor("historical_txns", Types.STRING())
        )
        self.today = ctx.get_list_state(
            ListStateDescriptor("today_txns", Types.STRING())
        )
        self.counter = ctx.get_state(ValueStateDescriptor("msg_count", Types.LONG()))
        self.start_ts = ctx.get_state(ValueStateDescriptor("start_time", Types.LONG()))
        self.last_seen = ctx.get_state(ValueStateDescriptor("last_seen", Types.LONG()))
        self.logged = ctx.get_state(ValueStateDescriptor("logged_once", Types.BOOLEAN()))

    def process_element(self, value, ctx):
        now_proc = ctx.timer_service().current_processing_time()
        self.today.add(value)

        # Initialize tracking states if first message
        if self.counter.value() is None:
            self.counter.update(0)
            self.start_ts.update(now_proc)
            self.logged.update(False)

        self.counter.update(self.counter.value() + 1)
        self.last_seen.update(now_proc)
        ctx.timer_service().register_processing_time_timer(now_proc + 10000)

        print(f"[{datetime.now()}] ✅ Message #{self.counter.value()} received:")
        print(f"Raw: {value}")

        # Handle transaction aggregation
        event_ts_ms = ctx.timestamp()
        if event_ts_ms is None:
            return

        txn_time = datetime.fromtimestamp(event_ts_ms / 1000.0)
        two_days_ago = txn_time - timedelta(days=2)

        txn = json.loads(value)
        user_id = txn['user_id']

        all_txns = list(self.historical.get()) + list(self.today.get())
        agg = defaultdict(float)

        for t in all_txns:
            t_parsed = json.loads(t)
            t_time = datetime.strptime(t_parsed['timestamp_str'], "%Y-%m-%d %H:%M:%S")
            if two_days_ago <= t_time <= txn_time:
                tx_type = t_parsed['transaction_type']
                direction = t_parsed['dep_or_withdraw']
                key = f"last_2d_tot_{tx_type}_{direction}"
                agg[key] += float(t_parsed['transaction_amount'])

        output_values = [user_id, two_days_ago, txn_time]
        for tx in TRANSACTION_TYPES:
            for dr in DIRECTION_TYPES:
                col_name = f"last_2d_tot_{tx}_{dr}"
                output_values.append(agg.get(col_name, 0.0))

        yield Row(*output_values)

    def on_timer(self, timestamp, ctx):
        now = ctx.timer_service().current_processing_time()
        last_seen = self.last_seen.value()
        already_logged = self.logged.value()

        if last_seen is not None and not already_logged and now - last_seen >= 10000:
            count = self.counter.value() or 0
            start_time = self.start_ts.value()
            if start_time is not None:
                elapsed = (now - start_time) / 1000.0
                # ✅ One-line final print
                print(f"📦 Total messages: {count} | ⏱️ Time taken: {elapsed:.2f} sec")
                self.logged.update(True)

        yield
