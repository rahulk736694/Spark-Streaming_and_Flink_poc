# # import os
# # from pyflink.datastream import StreamExecutionEnvironment
# # from pyflink.table import StreamTableEnvironment, EnvironmentSettings
# # from prettytable import PrettyTable

# # def print_pretty(table_result):
# #     column_names = table_result.get_table_schema().get_field_names()
# #     table = PrettyTable()
# #     table.field_names = column_names

# #     for row in table_result.collect():
# #         table.add_row(list(row))

# #     print(table)

# # def main():
# #     env = StreamExecutionEnvironment.get_execution_environment()
# #     settings = EnvironmentSettings.new_instance().in_batch_mode().build()  # batch mode
# #     t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# #     # Create HDFS table source
# #     t_env.execute_sql("""
# #         CREATE TABLE hdfs_input (
# #             user_id INT,
# #             transaction_amount DOUBLE,
# #             `timestamp` STRING,
# #             transaction_type STRING,
# #             transaction_id INT,
# #             event_time TIMESTAMP(3)
# #         ) WITH (
# #             'connector' = 'filesystem',
# #             'path' = 'hdfs://localhost:9000/flink_output/',
# #             'format' = 'csv'
# #         )
# #     """)

# #     # Query the HDFS table
# #     table_result = t_env.execute_sql("SELECT * FROM hdfs_input")

# #     # Print in table format
# #     print_pretty(table_result)

# # if __name__ == "__main__":
# #     main()
# import os
# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.table import StreamTableEnvironment, EnvironmentSettings
# from prettytable import PrettyTable


# def print_pretty(table_result):
#     column_names = table_result.get_table_schema().get_field_names()
#     table = PrettyTable()
#     table.field_names = column_names

#     for row in table_result.collect():
#         table.add_row(list(row))

#     print(table)


# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     settings = EnvironmentSettings.new_instance().in_batch_mode().build()
#     t_env = StreamTableEnvironment.create(env, environment_settings=settings)

#     # Define the HDFS CSV source table
#     t_env.execute_sql("""
#         CREATE TABLE hdfs_input (
#             user_id INT,
#             transaction_amount DOUBLE,
#             `timestamp` STRING,
#             transaction_type STRING,
#             transaction_id INT,
#             event_time TIMESTAMP(3)
#         ) WITH (
#             'connector' = 'filesystem',
#             'path' = 'hdfs://localhost:9000/flink_output/',
#             'format' = 'csv'
#         )
#     """)

#     # Filter only records in the last 7 days
#     t_env.execute_sql("""
#     CREATE TEMPORARY VIEW filtered_7d AS
#     SELECT * FROM hdfs_input
#     WHERE event_time >= TIMESTAMPADD(DAY, -7, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)))
# """)

#     # Aggregate totals for each transaction type per user
#     result = t_env.execute_sql("""
#         SELECT
#             user_id,
#             SUM(CASE WHEN transaction_type = 'atm_withd' THEN transaction_amount ELSE 0 END) AS last_7d_atm_withd,
#             SUM(CASE WHEN transaction_type = 'atm_dep' THEN transaction_amount ELSE 0 END) AS last_7d_atm_dep,
#             SUM(CASE WHEN transaction_type = 'wire_withd' THEN transaction_amount ELSE 0 END) AS last_7d_wire_withd,
#             SUM(CASE WHEN transaction_type = 'wire_dep' THEN transaction_amount ELSE 0 END) AS last_7d_wire_dep
#         FROM filtered_7d
#         GROUP BY user_id
#     """)

#     # Print to terminal
#     print_pretty(result)


# if __name__ == "__main__":
#     main()

import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from prettytable import PrettyTable


def print_pretty(table_result):
    column_names = table_result.get_table_schema().get_field_names()
    table = PrettyTable()
    table.field_names = column_names

    for row in table_result.collect():
        table.add_row(list(row))

    print(table)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_batch_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Source: HDFS table
    t_env.execute_sql("""
        CREATE TABLE hdfs_input (
            user_id INT,
            transaction_amount DOUBLE,
            `timestamp` STRING,
            transaction_type STRING,
            transaction_id INT,
            event_time TIMESTAMP(3)
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'hdfs://localhost:9000/flink_output1/',
            'format' = 'csv'
        )
    """)

    # Create 7d, 15d, 30d filtered views
    for days in [7, 15, 30]:
        t_env.execute_sql(f"""
            CREATE TEMPORARY VIEW filtered_{days}d AS
            SELECT * FROM hdfs_input
            WHERE event_time >= TIMESTAMPADD(DAY, -{days}, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)))
        """)

    # Join all results by user_id
    result = t_env.execute_sql("""
        SELECT
            COALESCE(a.user_id, b.user_id, c.user_id) AS user_id,

            -- atm_withd
            COALESCE(a.last_atm_withd, 0) AS last_7d_atm_withd,
            COALESCE(b.last_atm_withd, 0) AS last_15d_atm_withd,
            COALESCE(c.last_atm_withd, 0) AS last_30d_atm_withd,

            -- atm_dep
            COALESCE(a.last_atm_dep, 0) AS last_7d_atm_dep,
            COALESCE(b.last_atm_dep, 0) AS last_15d_atm_dep,
            COALESCE(c.last_atm_dep, 0) AS last_30d_atm_dep,

            -- wire_withd
            COALESCE(a.last_wire_withd, 0) AS last_7d_wire_withd,
            COALESCE(b.last_wire_withd, 0) AS last_15d_wire_withd,
            COALESCE(c.last_wire_withd, 0) AS last_30d_wire_withd,

            -- wire_dep
            COALESCE(a.last_wire_dep, 0) AS last_7d_wire_dep,
            COALESCE(b.last_wire_dep, 0) AS last_15d_wire_dep,
            COALESCE(c.last_wire_dep, 0) AS last_30d_wire_dep

        FROM

        (SELECT
            user_id,
            SUM(CASE WHEN transaction_type = 'atm_withd' THEN transaction_amount ELSE 0 END) AS last_atm_withd,
            SUM(CASE WHEN transaction_type = 'atm_dep' THEN transaction_amount ELSE 0 END) AS last_atm_dep,
            SUM(CASE WHEN transaction_type = 'wire_withd' THEN transaction_amount ELSE 0 END) AS last_wire_withd,
            SUM(CASE WHEN transaction_type = 'wire_dep' THEN transaction_amount ELSE 0 END) AS last_wire_dep
        FROM filtered_7d
        GROUP BY user_id) a

        FULL OUTER JOIN

        (SELECT
            user_id,
            SUM(CASE WHEN transaction_type = 'atm_withd' THEN transaction_amount ELSE 0 END) AS last_atm_withd,
            SUM(CASE WHEN transaction_type = 'atm_dep' THEN transaction_amount ELSE 0 END) AS last_atm_dep,
            SUM(CASE WHEN transaction_type = 'wire_withd' THEN transaction_amount ELSE 0 END) AS last_wire_withd,
            SUM(CASE WHEN transaction_type = 'wire_dep' THEN transaction_amount ELSE 0 END) AS last_wire_dep
        FROM filtered_15d
        GROUP BY user_id) b

        ON a.user_id = b.user_id

        FULL OUTER JOIN

        (SELECT
            user_id,
            SUM(CASE WHEN transaction_type = 'atm_withd' THEN transaction_amount ELSE 0 END) AS last_atm_withd,
            SUM(CASE WHEN transaction_type = 'atm_dep' THEN transaction_amount ELSE 0 END) AS last_atm_dep,
            SUM(CASE WHEN transaction_type = 'wire_withd' THEN transaction_amount ELSE 0 END) AS last_wire_withd,
            SUM(CASE WHEN transaction_type = 'wire_dep' THEN transaction_amount ELSE 0 END) AS last_wire_dep
        FROM filtered_30d
        GROUP BY user_id) c

        ON COALESCE(a.user_id, b.user_id) = c.user_id
    """)

    # Print the result
    print_pretty(result)


if __name__ == "__main__":
    main()

