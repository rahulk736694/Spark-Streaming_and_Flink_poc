import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table import RowKind
from prettytable import PrettyTable


def print_row(row):
    table = PrettyTable()
    table.field_names = [
        "user_id",
        "last_7d_atm_withd", "last_15d_atm_withd",
        "last_7d_atm_dep", "last_15d_atm_dep",
        "last_7d_wire_withd", "last_15d_wire_withd",
        "last_7d_wire_dep", "last_15d_wire_dep",
        "last_event_time", "updated_at"
    ]
    table.add_row(list(row))
    print(table)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    kafka_jar_path = os.path.abspath("flink-sql-connector-kafka-1.17.1.jar")
    t_env.get_config().get_configuration().set_string("pipeline.jars", f"file://{kafka_jar_path}")

    t_env.execute_sql("""
        CREATE TABLE transactions (
            transaction_id INT,
            transaction_amount DOUBLE,
            user_id INT,
            `timestamp` STRING,
            transaction_type STRING,
            event_time AS TO_TIMESTAMP(`timestamp`),
            WATERMARK FOR event_time AS event_time - INTERVAL '30' DAY
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'transactions-topic',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'flink-table-consumer',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    t_env.execute_sql("""
        CREATE TEMPORARY VIEW latest_user_event AS
        SELECT user_id, MAX(event_time) AS last_tx_time
        FROM TABLE(
            TUMBLE(TABLE transactions, DESCRIPTOR(event_time), INTERVAL '20' SECOND)
        )
        GROUP BY user_id
    """)

    t_env.execute_sql("""
        CREATE TEMPORARY VIEW user_aggregates AS
        SELECT
            user_id,

            SUM(CASE WHEN transaction_type = 'atm_withd' AND event_time >= CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) - INTERVAL '7' DAY THEN transaction_amount ELSE 0 END) AS last_7d_atm_withd,
            SUM(CASE WHEN transaction_type = 'atm_withd' AND event_time >= CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) - INTERVAL '15' DAY THEN transaction_amount ELSE 0 END) AS last_15d_atm_withd,

            SUM(CASE WHEN transaction_type = 'atm_dep' AND event_time >= CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) - INTERVAL '7' DAY THEN transaction_amount ELSE 0 END) AS last_7d_atm_dep,
            SUM(CASE WHEN transaction_type = 'atm_dep' AND event_time >= CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) - INTERVAL '15' DAY THEN transaction_amount ELSE 0 END) AS last_15d_atm_dep,

            SUM(CASE WHEN transaction_type = 'wire_withd' AND event_time >= CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) - INTERVAL '7' DAY THEN transaction_amount ELSE 0 END) AS last_7d_wire_withd,
            SUM(CASE WHEN transaction_type = 'wire_withd' AND event_time >= CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) - INTERVAL '15' DAY THEN transaction_amount ELSE 0 END) AS last_15d_wire_withd,

            SUM(CASE WHEN transaction_type = 'wire_dep' AND event_time >= CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) - INTERVAL '7' DAY THEN transaction_amount ELSE 0 END) AS last_7d_wire_dep,
            SUM(CASE WHEN transaction_type = 'wire_dep' AND event_time >= CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) - INTERVAL '15' DAY THEN transaction_amount ELSE 0 END) AS last_15d_wire_dep,

            MAX(event_time) AS last_event_time,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS updated_at

        FROM transactions
        GROUP BY user_id
    """)

    result_table = t_env.sql_query("""
        SELECT a.*
        FROM user_aggregates a
        JOIN latest_user_event b ON a.user_id = b.user_id
    """)

    print("Flink job started. Waiting for Kafka transactions...\n")
    print("Collecting result rows...")

    with result_table.execute().collect() as results:
        for row in results:
            if row.get_row_kind() in (RowKind.INSERT, RowKind.UPDATE_AFTER):
                print_row(row)


if __name__ == '__main__':
    main()
