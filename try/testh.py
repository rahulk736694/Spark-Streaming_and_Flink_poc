# import os
# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.enable_checkpointing(10000)

#     settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
#     t_env = StreamTableEnvironment.create(env, environment_settings=settings)

#     # Add Kafka and JSON format jars
#     kafka_jar = os.path.abspath("flink-sql-connector-kafka-1.17.1.jar")
#     json_jar = os.path.abspath("flink-json-1.17.1.jar")
#     t_env.get_config().get_configuration().set_string(
#         "pipeline.jars", f"file://{kafka_jar};file://{json_jar}"
#     )

#     # Kafka source table
#     t_env.execute_sql("""
#         CREATE TABLE transactions (
#             user_id INT,
#             transaction_amount DOUBLE,
#             `timestamp` STRING,
#             transaction_type STRING,
#             transaction_id INT,
#             proctime AS PROCTIME(),
#             event_time AS TO_TIMESTAMP(`timestamp`),
#             WATERMARK FOR event_time AS event_time - INTERVAL '99' DAY
#         ) WITH (
#             'connector' = 'kafka',
#             'topic' = 'transactions-topic',
#             'properties.bootstrap.servers' = 'localhost:9092',
#             'properties.group.id' = 'flink-table-consumer',
#             'scan.startup.mode' = 'latest-offset',
#             'format' = 'json',
#             'json.fail-on-missing-field' = 'false',
#             'json.ignore-parse-errors' = 'true'
#         )
#     """)

#     # HDFS sink table
#     t_env.execute_sql("""
#         CREATE TABLE hdfs_sink (
#             user_id INT,
#             transaction_amount DOUBLE,
#             `timestamp` STRING,
#             transaction_type STRING,
#             transaction_id INT,
#             event_time TIMESTAMP(3)
#         ) WITH (
#             'connector' = 'filesystem',
#             'path' = 'hdfs://localhost:9000/flink_output/',
#             'format' = 'csv',
#             'auto-compaction' = 'true',
#             'sink.rolling-policy.file-size' = '1MB',
#             'sink.rolling-policy.rollover-interval' = '1 min',
#             'sink.rolling-policy.check-interval' = '30 sec'
#         )
#     """)

#     # Insert data from Kafka to HDFS
#     t_env.execute_sql("""
#         INSERT INTO hdfs_sink
#         SELECT user_id, transaction_amount, `timestamp`, transaction_type, transaction_id, event_time
#         FROM transactions
#     # """)

# if __name__ == '__main__':
#     main()

import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Add Kafka and JSON format jars
    kafka_jar = os.path.abspath("flink-sql-connector-kafka-1.17.1.jar")
    json_jar = os.path.abspath("flink-json-1.17.1.jar")
    t_env.get_config().get_configuration().set_string(
        "pipeline.jars", f"file://{kafka_jar};file://{json_jar}"
    )

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
            WATERMARK FOR event_time AS event_time - INTERVAL '99' DAY
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

    # HDFS sink table
    t_env.execute_sql("""
        CREATE TABLE hdfs_sink (
            user_id INT,
            transaction_amount DOUBLE,
            `timestamp` STRING,
            transaction_type STRING,
            transaction_id INT,
            event_time TIMESTAMP(3)
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'hdfs://localhost:9000/flink_output/',
            'format' = 'csv',
            'auto-compaction' = 'true',
            'sink.rolling-policy.file-size' = '1MB',
            'sink.rolling-policy.rollover-interval' = '1 min',
            'sink.rolling-policy.check-interval' = '30 sec'
        )
    """)

    # Insert data from Kafka to HDFS
    t_env.execute_sql("""
        INSERT INTO hdfs_sink
        SELECT user_id, transaction_amount, `timestamp`, transaction_type, transaction_id, event_time
        FROM transactions
    # """)

if __name__ == '__main__':
    main()











