import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, RowKind
from prettytable import PrettyTable


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Register Kafka connector JAR
    kafka_jar_path = os.path.abspath("flink-sql-connector-kafka-1.17.1.jar")
    t_env.get_config().get_configuration().set_string("pipeline.jars", f"file://{kafka_jar_path}")

    # Kafka source table
    t_env.execute_sql("""
        CREATE TABLE transactions (
            user_id INT,
            transaction_amount DOUBLE,
            `timestamp` STRING,
            transaction_type STRING,
            transaction_id INT,
            proctime AS PROCTIME(),
            event_time AS TO_TIMESTAMP(`timestamp`),
            WATERMARK FOR event_time AS event_time - INTERVAL '30' DAY
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'transactions-topic',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'flink-table-consumer',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    #
    result_table = t_env.sql_query("SELECT * FROM transactions")

    with result_table.execute().collect() as results:
        for row in results:
            print(row)

    print("Flink Table API job with per-type sliding window aggregation is running...\n")


if __name__ == '__main__':
    main()
