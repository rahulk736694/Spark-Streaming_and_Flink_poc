

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor
from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common import Duration, Row
from pyflink.common import Time
from pyflink.datastream.state import StateTtlConfig
from datetime import datetime, timedelta
from collections import defaultdict
import json
import time

from config import *

HISTORY_LOAD_START_TS = None
processed_count = 0
throughput_start = time.time()

class JsonTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp):
        try:
            ts_str = json.loads(element)['timestamp_str']
            dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            return int(dt.timestamp() * 1000)
        except Exception as e:
            print(f"[WARN] Timestamp assign failed: {e}")
        return 0


class StoreAndAggregateUserTxns(KeyedProcessFunction):
    def open(self, ctx: RuntimeContext):
        ttl_config = StateTtlConfig.new_builder(Time.days(STATE_TTL_DAYS)) \
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired) \
            .build()

        descriptor = MapStateDescriptor("user_txns", Types.STRING(), Types.STRING())
        descriptor.enable_time_to_live(ttl_config)
        self.user_txns = ctx.get_map_state(descriptor)

    def process_element(self, value, ctx):
        user_id, data = value

        if isinstance(data, Row):  
            txn_json = data.raw_json
            parsed = json.loads(txn_json)
            self.user_txns.put(str(parsed['transaction_id']), txn_json)
            print(f"[LOAD HIST] user={parsed['user_id']} → {parsed['timestamp_str']}")
            return

        parsed = json.loads(data)
        txn_id = parsed['transaction_id']
        user_id = parsed['user_id']
        self.user_txns.put(str(txn_id), data)
        msg_event_ts = datetime.strptime(parsed['timestamp_str'], "%Y-%m-%d %H:%M:%S")
        process_ts = datetime.now()
        latency_sec = (process_ts - msg_event_ts).total_seconds()
        
        print(f"[LATENCY] user={user_id} txn_id={txn_id} latency={latency_sec:.3f} sec")
        
        # === THROUGHPUT MEASUREMENT ===
        global processed_count, throughput_start
        processed_count += 1
        if processed_count % 1000 == 0:
            elapsed = time.time() - throughput_start
            throughput = processed_count / elapsed
            print(f"[THROUGHPUT] {processed_count} messages in {elapsed:.2f}s → {throughput:.2f} msg/sec")

        ts_millis = ctx.timestamp()
        if ts_millis is None or ts_millis < 0:
            return

        event_time = datetime.fromtimestamp(ts_millis / 1000.0)
        window_start = event_time - timedelta(days=ROLLING_WINDOW_DAYS)

        agg = defaultdict(lambda: {'sum': 0.0, 'count': 0, 'min': float('inf'), 'max': float('-inf')})
        for txn_json in list(self.user_txns.values()):
            try:
                data = json.loads(txn_json)
                ts = datetime.strptime(data['timestamp_str'], "%Y-%m-%d %H:%M:%S")
                if window_start <= ts <= event_time:
                    key = (data['transaction_type'], data['dep_or_withdraw'])
                    amount = float(data['transaction_amount'])
                    agg[key]['sum'] += amount
                    agg[key]['count'] += 1
                    agg[key]['min'] = min(agg[key]['min'], amount)
                    agg[key]['max'] = max(agg[key]['max'], amount)

            except Exception as e:
                print(f"[AGG ERROR] {e}")

        result = []
        for t in TRANSACTION_TYPES:
            for d in DIRECTION_TYPES:
                stats = agg.get((t, d), {'sum': 0.0, 'count': 0, 'min': 0.0, 'max': 0.0})
                avg_val = stats['sum'] / stats['count'] if stats['count'] > 0 else 0.0
                result.extend([stats['sum'], avg_val, stats['max'], stats['min']])
        duration = time.time() - HISTORY_LOAD_START_TS
        print(f"[TIMER] user={user_id} total processing time: {round(duration, 3)}s")
        yield Row(user_id, window_start, event_time, *result)



def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.set_state_backend(EmbeddedRocksDBStateBackend())
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    t_env = StreamTableEnvironment.create(env)
    st=time.time()
    # === HDFS Source Table ===
    t_env.execute_sql(f"""
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
            'path' = '{HDFS_RAW_TXN_PATH}',
            'format' = 'csv'
        )
    """)

    global HISTORY_LOAD_START_TS
    

    hist_stream = t_env.to_data_stream(t_env.from_path("hdfs_raw_transactions"))
    end=time.time()
    print(f"[TIMER] total History message loading time: {round(end-st, 3)}s")
    HISTORY_LOAD_START_TS = time.time()
    # Kafka Source
    kafka_props = {
        'bootstrap.servers': KAFKA_BROKERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'latest'
    }
    kafka_source = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    kafka_stream = env.add_source(kafka_source).assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(WATERMARK_DELAY_MINUTES))
        .with_timestamp_assigner(JsonTimestampAssigner())
    )

    row_stream = kafka_stream.map(
        lambda x: Row(
            int(json.loads(x)['user_id']),
            float(json.loads(x)['transaction_amount']),
            json.loads(x)['dep_or_withdraw'],
            json.loads(x)['timestamp_str'],
            int(json.loads(x)['transaction_id']),
            json.loads(x)['transaction_type'],
            x,
            datetime.now().strftime("%Y-%m-%d")
        ),
        output_type=Types.ROW_NAMED(
            ['user_id', 'transaction_amount', 'dep_or_withdraw', 'timestamp_str',
             'transaction_id', 'transaction_type', 'raw_json', 'dt'],
            [Types.INT(), Types.FLOAT(), Types.STRING(), Types.STRING(),
             Types.INT(), Types.STRING(), Types.STRING(), Types.STRING()]
        )
    )
    t_env.create_temporary_view("kafka_txn_rows", row_stream)

    # Write Kafka to HDFS
    t_env.execute_sql(f"""
        CREATE TABLE kafka_txn_append (
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
            'path' = '{HDFS_RAW_TXN_PATH}',
            'format' = 'csv',
            'sink.partition-commit.policy.kind' = 'success-file',
            'sink.partition-commit.trigger' = 'process-time',
            'sink.partition-commit.delay' = '5 second'
        )
    """)
    t_env.execute_sql("INSERT INTO kafka_txn_append SELECT * FROM kafka_txn_rows")

    # Union and process
    tagged_hist = hist_stream.map(
        lambda row: (row.user_id, row.raw_json),
        output_type=Types.TUPLE([Types.INT(), Types.STRING()])
    )
    tagged_kafka = kafka_stream.map(
        lambda x: (json.loads(x)['user_id'], x),
        output_type=Types.TUPLE([Types.INT(), Types.STRING()])
    )

    unioned_stream = tagged_hist.union(tagged_kafka)

    output_fields = ['user_id', 'window_start', 'window_end']
    output_types = [Types.INT(), Types.SQL_TIMESTAMP(), Types.SQL_TIMESTAMP()]
    for t in TRANSACTION_TYPES:
        for d in DIRECTION_TYPES:
            prefix = f"last_{ROLLING_WINDOW_DAYS}d_{t}_{d}"
            output_fields.extend([
                 f"{prefix}_sum", f"{prefix}_avg", f"{prefix}_max", f"{prefix}_min"
            ])
            output_types.extend([Types.FLOAT()] * 4)


    aggregated = unioned_stream.key_by(lambda x: x[0], key_type=Types.INT()) \
    .process(StoreAndAggregateUserTxns(), output_type=Types.ROW_NAMED(output_fields, output_types))

    t_env.create_temporary_view("user_2d_agg", aggregated)

    # Final sink
    columns_def = ",\n            ".join([f"{col} FLOAT" for col in output_fields[3:]])  
    t_env.execute_sql(f"""
    CREATE TABLE daily_user_total (
        user_id INT,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        {columns_def}
    )
    WITH (
        'connector' = 'filesystem',
        'path' = '{HDFS_KI_OUTPUT_PATH}',
        'format' = 'csv',
        'sink.partition-commit.policy.kind' = 'success-file',
        'sink.partition-commit.trigger' = 'process-time',
        'sink.partition-commit.delay' = '5 second'
    )
""")
    t_env.execute_sql("INSERT INTO daily_user_total SELECT * FROM user_2d_agg")

    env.execute("Flink: Modular RocksDB + Kafka + HDFS + Union Aggregation")


if __name__ == "__main__":
    main()
