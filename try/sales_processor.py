import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    tbl_env = StreamTableEnvironment.create(env, environment_settings=settings)

    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                             'flink-sql-connector-kafka-1.17.1.jar')
    tbl_env.get_config().get_configuration().set_string("pipeline.jars", f"file://{kafka_jar}")

    # Kafka Source Table
    tbl_env.execute_sql("""
        CREATE TABLE transactions (
            transaction_id INT,
            transaction_amount DOUBLE,
            user_id INT,
            `timestamp` BIGINT,
            transaction_type STRING,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'transactions',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'transactions-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    intervals = [1, 2, 3, 5, 10, 20, 30]  # Seconds
    for interval in intervals:
        tbl_env.execute_sql(f"""
            CREATE TABLE agg_{interval}s (
                user_id INT,
                transaction_type STRING,
                window_end TIMESTAMP(3),
                total_amount DOUBLE
            ) WITH (
                'connector' = 'print'
            )
        """)

        tbl_env.execute_sql(f"""
            INSERT INTO agg_{interval}s
            SELECT
                user_id,
                transaction_type,
                TUMBLE_END(proctime, INTERVAL '{interval}' SECOND) AS window_end,
                SUM(transaction_amount) AS total_amount
            FROM transactions
            GROUP BY TUMBLE(proctime, INTERVAL '{interval}' SECOND), user_id, transaction_type
        """)

    # Last transaction table per user_id and transaction_type
    tbl_env.execute_sql("""
        CREATE TABLE last_transaction (
            user_id INT,
            transaction_type STRING,
            last_transaction_amount DOUBLE,
            last_timestamp BIGINT
        ) WITH (
            'connector' = 'print'
        )
    """)

    tbl_env.execute_sql("""
        INSERT INTO last_transaction
        SELECT
            user_id,
            transaction_type,
            LAST_VALUE(transaction_amount) OVER (PARTITION BY user_id, transaction_type ORDER BY proctime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_transaction_amount,
            LAST_VALUE(`timestamp`) OVER (PARTITION BY user_id, transaction_type ORDER BY proctime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_timestamp
        FROM transactions
    """)

    print("Streaming job started")

if __name__ == '__main__':
    main()
