from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import *
from pyflink.table.window import Tumble

#####################################################################
# 1
# Create a TableEnvironment
# Using Table API for manipulation 
#####################################################################

def log_processing():

    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    
    ##### specify connector and format jars
    t_env.get_config().set("pipeline.jars", "file:///app/flink-sql-connector-kafka-1.17.0.jar")
    t_env.get_config().set("table.exec.source.idle-timeout", "1000")


    # FX Source 
    source_ddl1 = """
        CREATE TABLE source_table_fx(
            timez BIGINT,
            fx_rate DOUBLE,
            timez_ltz AS TO_TIMESTAMP_LTZ(timez,3),
            WATERMARK FOR timez_ltz AS timez_ltz - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'stream2fx',
            'properties.bootstrap.servers' = 'broker:29092',
            'properties.group.id' = 'fx_group',
            'scan.startup.mode' = 'specific-offsets',
            'scan.startup.specific-offsets' = 'partition:0,offset:0',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true',
            'format' = 'json'
        )
        """
    t_env.execute_sql(source_ddl1)
    tbl1 = t_env.from_path('source_table_fx')

     # WS Source 
    source_ddl2 = """
        CREATE TABLE source_table_ws(
            timez BIGINT,
            price DOUBLE,
            timez_ltz AS TO_TIMESTAMP_LTZ(timez,3),
            WATERMARK FOR timez_ltz AS timez_ltz - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'stream1ws',
            'properties.bootstrap.servers' = 'broker:29092',
            'properties.group.id' = 'ws_group',
            'scan.startup.mode' = 'specific-offsets',
            'scan.startup.specific-offsets' = 'partition:0,offset:0',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true',
            'format' = 'json'
        )
        """
    t_env.execute_sql(source_ddl2)
    tbl2 = t_env.from_path('source_table_ws') 

    windowed_fx = (
    tbl1.window(Tumble.over(lit(5).seconds).on(col("timez_ltz")).alias("w"))
    .group_by(col("w"))
    .select(
        col("w").start.alias("window_start_fx"),
        col("w").end.alias("window_end_fx"),
        col("fx_rate").avg.alias("avg_fx_rate"),
        col("fx_rate").count.alias("count_fx")
        )
    )

    windowed_ws = (
    tbl2.window(Tumble.over(lit(5).seconds).on(col("timez_ltz")).alias("w"))
    .group_by(col("w"))
    .select(
        col("w").start.alias("window_start_ws"),
        col("w").end.alias("window_end_ws"),
        col("price").avg.alias("avg_price"),
        col("price").count.alias("count_ws")
        )
    )
    
    joined_table = windowed_fx.join(windowed_ws).where(col('window_start_fx') == col('window_start_ws')).select(col('window_start_ws'), col('avg_fx_rate'), col('avg_price'))
    result_table = joined_table.select(
        col("window_start_ws"),
        col("avg_fx_rate"),
        col("avg_price"),
        col("avg_fx_rate") * col("avg_price")).alias("result")
    
    # Option 2
    # sink_ddl_final = """
    #     CREATE TABLE printa (
    #     `window_start_ws` TIMESTAMP(3),
    #     `avg_fx_rate` DOUBLE,
    #     `avg_price` DOUBLE,
    #     `result` DOUBLE
    # ) WITH (
    #     'connector' = 'print'
    # )
    # """
    # t_env.execute_sql(sink_ddl_final)
    # result_table.execute_insert('printa').wait()


    # Define the Kafka sink
    sink_ddl_kafka = """
        CREATE TABLE sink_kafka (
            `window_start_ws` TIMESTAMP(3),
            `avg_fx_rate` DOUBLE,
            `avg_price` DOUBLE,
            `result` DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'enriched',
            'properties.bootstrap.servers' = 'broker:29092',
            'format' = 'json'
        )
    """
    # Execute the Kafka sink DDL
    t_env.execute_sql(sink_ddl_kafka)

    # Write the result table to the Kafka sink
    result_table.execute_insert("sink_kafka").wait()
    
if __name__ == '__main__':
    log_processing()
