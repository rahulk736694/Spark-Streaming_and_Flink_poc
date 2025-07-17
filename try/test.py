import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    
    kafka_jar_path = os.path.abspath("flink-sql-connector-kafka-1.17.1.jar")
    t_env.get_config().get_configuration().set_string("pipeline.jars", f"file://{kafka_jar_path}")

    
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
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    t_env.execute_sql("SELECT * FROM transactions").print()

if __name__ == '__main__':
    main()
