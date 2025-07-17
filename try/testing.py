import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Register Kafka JAR
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'flink-sql-connector-kafka-1.17.1.jar')
    table_env.get_config().get_configuration().set_string("pipeline.jars", f"file://{kafka_jar}")

    # Kafka source table
    table_env.execute_sql("""
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
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    table_env.execute_sql("""
       CREATE TABLE dashboard_table (
        user_id INT,
        last_1s_total_atm_withd DOUBLE,
        last_1s_total_atm_dep DOUBLE,
        last_1s_total_upi_withd DOUBLE,
        last_1s_total_upi_dep DOUBLE,
        last_1s_total_pos_withd DOUBLE,
        last_1s_total_pos_dep DOUBLE,
        last_1s_total_check_withd DOUBLE,
        last_1s_total_check_dep DOUBLE
    ) WITH (
            'connector' = 'kafka',
            'topic' = 'transactions',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'transactions-group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
    )
""")

    table_env.execute_sql("""
        INSERT INTO dashboard_table
        SELECT
        user_id,
        SUM(CASE WHEN transaction_type = 'atm_withd' THEN transaction_amount ELSE 0 END) AS last_1s_total_atm_withd,
        SUM(CASE WHEN transaction_type = 'atm_dep' THEN transaction_amount ELSE 0 END) AS last_1s_total_atm_dep,
        SUM(CASE WHEN transaction_type = 'wire_withd' THEN transaction_amount ELSE 0 END) AS last_1s_total_wire_withd,
        SUM(CASE WHEN transaction_type = 'wire_dep' THEN transaction_amount ELSE 0 END) AS last_1s_total_wire_dep,
        SUM(CASE WHEN transaction_type = 'ach_withd' THEN transaction_amount ELSE 0 END) AS last_1s_total_ach_withd,
        SUM(CASE WHEN transaction_type = 'ach_dep' THEN transaction_amount ELSE 0 END) AS last_1s_total_ach_dep,
        SUM(CASE WHEN transaction_type = 'check_withd' THEN transaction_amount ELSE 0 END) AS last_1s_total_check_withd,
        SUM(CASE WHEN transaction_type = 'check_dep' THEN transaction_amount ELSE 0 END) AS last_1s_total_check_dep
        FROM transactions
        GROUP BY
        user_id,
        TUMBLE(proctime, INTERVAL '5' MINUTE)
    """)

    table_result = table_env.execute_sql("""
    SELECT 
        user_id, 
        SUM(transaction_amount) AS total_amount, 
        TUMBLE_START(proctime, INTERVAL '1' MINUTE) AS window_start,
        TUMBLE_END(proctime, INTERVAL '1' MINUTE) AS window_end
    FROM transactions
    GROUP BY user_id, TUMBLE(proctime, INTERVAL '1' MINUTE)
""")

    table_result.print()
    

    # print(table_env.execute_sql("SELECT * FROM dashboard_table LIMIT 10").print())
    print(table_env.execute_sql("SELECT * FROM transactions LIMIT 10").print())
    print("Flink job is running. Input from Kafka will print below...\n")

if __name__ == '__main__':
    main()
