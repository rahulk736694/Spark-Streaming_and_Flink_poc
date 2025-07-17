
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor, MapState
from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common import Duration, Row
from datetime import datetime, timedelta
from collections import defaultdict
from pyflink.datastream.state import MapStateDescriptor, StateTtlConfig
from pyflink.common import Time
import time 
HISTORY_LOAD_START_TS = None
import json

TRANSACTION_TYPES = ['atm', 'wire', 'check', 'plp']
DIRECTION_TYPES = ['dep', 'credit']


class JsonTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp):
        if isinstance(element, str):
            try:
                ts_str = json.loads(element)['timestamp_str']
                dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                return int(dt.timestamp() * 1000)
            except Exception as e:
                print(f"[WARN] Failed timestamp assign: {e}")
        return 0


class StoreAndAggregateUserTxns(KeyedProcessFunction):
    def open(self, ctx: RuntimeContext):
        ttl_config = StateTtlConfig.new_builder(Time.days(2)) \
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired) \
            .build()

        descriptor = MapStateDescriptor("user_txns", Types.STRING(), Types.STRING())
        descriptor.enable_time_to_live(ttl_config)

        self.user_txns = ctx.get_map_state(descriptor)

    def process_element(self, value, ctx):
        if isinstance(value, tuple):
            user_id, data = value
            if isinstance(data, Row):
                txn_json = data.raw_json
                parsed = json.loads(txn_json)
                self.user_txns.put(str(parsed['transaction_id']), txn_json)
                print(f"[LOAD HIST] user={parsed['user_id']} → {parsed['timestamp_str']}")
                return
            elif isinstance(data, str):
                parsed = json.loads(data)
                txn_id = parsed['transaction_id']
                user_id = parsed['user_id']
                self.user_txns.put(str(txn_id), data)
                # print(f"[KAFKA] user={user_id} → {parsed['timestamp_str']}")
                # print(f"[UNION UPDATE] Total txns for user {user_id}: {len(list(self.user_txns.keys()))}")

                ts_millis = ctx.timestamp()
                if ts_millis is None or ts_millis < 0:
                    # print(f"[SKIP AGG] Invalid event time for user={user_id}: {ts_millis}")
                    return

                event_time = datetime.fromtimestamp(ts_millis / 1000.0)
                window_start = event_time - timedelta(days=2)

                agg = defaultdict(float)
                count = 0
                # print(f"[AGG INIT] user={user_id}, window={window_start} to {event_time}")
                # print(f"[STATE] user={user_id} has {len(list(self.user_txns.keys()))} txns in RocksDB")

                for txn_json in list(self.user_txns.values()):
                    try:
                        data = json.loads(txn_json)
                        ts = datetime.strptime(data['timestamp_str'], "%Y-%m-%d %H:%M:%S")
                        # print(f"[DEBUG TXN] ts={ts}, user={data['user_id']}, amt={data['transaction_amount']}, type={data['transaction_type']}, dir={data['dep_or_withdraw']}")
                        if window_start <= ts <= event_time:
                            key = (data['transaction_type'], data['dep_or_withdraw'])
                            agg[key] += float(data['transaction_amount'])
                            count += 1
                    except Exception as e:
                        print(f"[AGG ERROR] {e}")

                totals = [agg[(t, d)] for t in TRANSACTION_TYPES for d in DIRECTION_TYPES]
                # print(f"[AGG DONE] user={user_id}, count={count}, totals={totals}")
                duration = time.time() - HISTORY_LOAD_START_TS
                print(f"[TIMER] user={user_id} total processing time: {round(duration, 3)} seconds")

                yield Row(user_id, window_start, event_time, *totals)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.set_state_backend(EmbeddedRocksDBStateBackend())
    env.enable_checkpointing(60000)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    t_env = StreamTableEnvironment.create(env)

    # === HDFS source table ===
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
            'path' = 'hdfs://localhost:9000/flink/raw_transactions_testing/',
            'format' = 'csv'
        )
    """)
    global HISTORY_LOAD_START_TS
    HISTORY_LOAD_START_TS = time.time()

    hist_stream = t_env.to_data_stream(t_env.from_path("hdfs_raw_transactions"))

    kafka_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'txn-user-agg-group'
    }
    kafka_source = FlinkKafkaConsumer(
        topics='transactions-topic',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    kafka_stream = env.add_source(kafka_source).assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(5))
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

    t_env.execute_sql("""
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
            'path' = 'hdfs://localhost:9000/flink/raw_transactions_testing/',
            'format' = 'csv',
            'sink.partition-commit.policy.kind' = 'success-file',
            'sink.partition-commit.trigger' = 'process-time',
            'sink.partition-commit.delay' = '5 second'
        )
    """)
    t_env.execute_sql("INSERT INTO kafka_txn_append SELECT * FROM kafka_txn_rows")

    hist_row_type = Types.ROW_NAMED(
        ['user_id', 'transaction_amount', 'dep_or_withdraw', 'timestamp_str',
         'transaction_id', 'transaction_type', 'raw_json', 'dt'],
        [Types.INT(), Types.FLOAT(), Types.STRING(), Types.STRING(),
         Types.INT(), Types.STRING(), Types.STRING(), Types.STRING()]
    )

    tagged_hist = hist_stream.map(
    lambda row: (row.user_id, row.raw_json),  # convert Row to raw_json string
    output_type=Types.TUPLE([Types.INT(), Types.STRING()])
)
    tagged_kafka = kafka_stream.map(lambda x: (json.loads(x)['user_id'], x),
                                    output_type=Types.TUPLE([Types.INT(), Types.STRING()]))

    unioned_stream = tagged_hist.union(tagged_kafka)

    aggregated = unioned_stream.key_by(lambda x: x[0], key_type=Types.INT()) \
        .process(StoreAndAggregateUserTxns(), output_type=Types.ROW_NAMED(
            ['user_id', 'window_start', 'window_end'] +
            [f"last_2d_tot_{t}_{d}" for t in TRANSACTION_TYPES for d in DIRECTION_TYPES],
            [Types.INT(), Types.SQL_TIMESTAMP(), Types.SQL_TIMESTAMP()] + [Types.FLOAT()] * 8
        ))

    t_env.create_temporary_view("user_2d_agg", aggregated)

    t_env.execute_sql("""
        CREATE TABLE daily_user_total (
            user_id INT,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            last_2d_tot_atm_dep FLOAT,
            last_2d_tot_atm_credit FLOAT,
            last_2d_tot_wire_dep FLOAT,
            last_2d_tot_wire_credit FLOAT,
            last_2d_tot_check_dep FLOAT,
            last_2d_tot_check_credit FLOAT,
            last_2d_tot_plp_dep FLOAT,
            last_2d_tot_plp_credit FLOAT
        )
        WITH (
            'connector' = 'filesystem',
            'path' = 'hdfs://localhost:9000/flink/KI_Indicators/',
            'format' = 'csv',
            'sink.partition-commit.policy.kind' = 'success-file',
            'sink.partition-commit.trigger' = 'process-time',
            'sink.partition-commit.delay' = '5 second'
        )
    """)
    t_env.execute_sql("INSERT INTO daily_user_total SELECT * FROM user_2d_agg")

    env.execute("Flink: RocksDB + Kafka + HDFS + History Aggregation")


if __name__ == "__main__":
    main()
