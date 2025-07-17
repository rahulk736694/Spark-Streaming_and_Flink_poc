# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.table import StreamTableEnvironment
# from pyflink.datastream.state import ValueStateDescriptor
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from pyflink.common.typeinfo import Types

# class RocksDBStoreFunction(KeyedProcessFunction):

#     def open(self, runtime_context: RuntimeContext):
#         # Define a ValueState to hold the latest raw_json string per user_id
#         state_descriptor = ValueStateDescriptor("raw_json_state", Types.STRING())
#         self.raw_json_state = runtime_context.get_state(state_descriptor)

#     def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
#         # value is a Row, access raw_json field (assume last column)
#         raw_json = value.raw_json

#         # Store/overwrite in RocksDB state per key (user_id)
#         self.raw_json_state.update(raw_json)

#         # For demo, print current state
#         current = self.raw_json_state.value()
#         print(f"user_id={ctx.get_current_key()} stored raw_json={current}")

#         # You can emit or do further processing if needed

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)
#     t_env = StreamTableEnvironment.create(env)

#     # Create the HDFS table as per your DDL
#     t_env.execute_sql("""
#         CREATE TABLE hdfs_raw_transactions (
#             user_id INT,
#             transaction_amount FLOAT,
#             dep_or_withdraw STRING,
#             timestamp_str STRING,
#             transaction_id INT,
#             transaction_type STRING,
#             raw_json STRING,
#             dt STRING
#         ) PARTITIONED BY (dt)
#         WITH (
#             'connector' = 'filesystem',
#             'path' = 'hdfs://localhost:9000/flink/transactions_raw/',
#             'format' = 'csv',
#             'sink.partition-commit.policy.kind' = 'success-file',
#             'sink.partition-commit.trigger' = 'process-time',
#             'sink.partition-commit.delay' = '10 sec',
#             'sink.rolling-policy.file-size' = '128MB',
#             'sink.rolling-policy.rollover-interval' = '1 min',
#             'sink.rolling-policy.check-interval' = '30 sec'
#         )
#     """)

#     # Read table
#     raw_txns = t_env.from_path("hdfs_raw_transactions")

#     # Convert to DataStream (type is Row, with attributes named as in schema)
#     ds = t_env.to_data_stream(raw_txns)

#     # Key by user_id (index 0)
#     keyed = ds.key_by(lambda row: row.user_id)

#     # Process with RocksDB backed state function
#     keyed.process(RocksDBStoreFunction())

#     env.execute("Store raw transactions in RocksDB state keyed by user_id")

# if __name__ == "__main__":
#     main()

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor
from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
from pyflink.common.typeinfo import Types
from pyflink.common import Row



# ============================
# RocksDB1 (historical) State Storer
# ============================
class HistoricalTxnStorer(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        self.historical_txn_state = runtime_context.get_list_state(
            ListStateDescriptor("yesterday_txns", Types.STRING())  # You can rename if needed
        )

    def process_element(self, value: Row, ctx):
        self.historical_txn_state.add(value.raw_json)
        # Optional log
        # print(f"[RocksDB1] user_id={ctx.get_current_key()} → stored txn")


# ============================
# Main Job: HDFS (all partitions) → RocksDB1
# ============================
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_state_backend(EmbeddedRocksDBStateBackend())
    env.enable_checkpointing(60000)

    t_env = StreamTableEnvironment.create(env)

    # Create table over all HDFS partitions
    t_env.execute_sql("""
        CREATE TABLE hdfs_raw_transactions (
            user_id INT,
            transaction_amount FLOAT,
            dep_or_withdraw STRING,
            timestamp_str STRING,
            transaction_id INT,
            transaction_type STRING,
            raw_json STRING,
            dt STRING
        ) PARTITIONED BY (dt)
        WITH (
            'connector' = 'filesystem',
            'path' = 'hdfs://localhost:9000/flink/transactions_raw/',
            'format' = 'csv'
        )
    """)

    # Read all partitions (entire HDFS transaction history)
    raw_txns_table = t_env.from_path("hdfs_raw_transactions")

    # Convert to DataStream
    ds = t_env.to_data_stream(raw_txns_table)

    # Key by user_id and store in RocksDB1
    keyed = ds.key_by(lambda row: row.user_id)
    keyed.process(HistoricalTxnStorer())

    env.execute("Load Full HDFS Transaction History → RocksDB1")


if __name__ == "__main__":
    main()
