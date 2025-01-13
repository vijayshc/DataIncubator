from pyspark.sql import SparkSession
from impala.dbapi import connect
import pandas as pd
import pydoop.hdfs as hdfs
from io import BytesIO

def get_spark_session(config):
    """Creates and returns a SparkSession based on the configuration."""

    if config.get('processing', 'method') == 'read_hive_hwc':
        spark = SparkSession.builder \
            .appName("DataProcessingApp") \
            .config("spark.sql.hive.hiveserver2.jdbc.url", config.get("hive", "hiveserver2_jdbc_url")) \
            .config("spark.datasource.hive.warehouse.metastoreUri", config.get("hive", "metastore_uri")) \
            .config("spark.hadoop.hive.llap.daemon.service.hosts", config.get("hive", "llap_daemon_service_hosts")) \
            .config("spark.driver.extraClassPath", config.get("hive", "driver_extraClassPath")) \
            .config("spark.executor.extraClassPath", config.get("hive", "executor_extraClassPath")) \
            .enableHiveSupport() \
            .getOrCreate()
    else:
        spark = SparkSession.builder \
            .appName("DataProcessingApp") \
            .config("spark.sql.catalogImplementation", "hive") \
            .enableHiveSupport() \
            .getOrCreate()
    return spark

def read_hive_with_spark(spark, config):
    """Reads a Hive table using Spark SQL."""
    db_name = config.get('hive', 'database')
    table_name = config.get('hive', 'table')
    df = spark.sql(f"SELECT * FROM {db_name}.{table_name}")
    return df

def read_hive_with_hwc(spark, config):
    """Reads a Hive table using Spark and Hive Warehouse Connector."""
    db_name = config.get('hive', 'database')
    table_name = config.get('hive', 'table')
    df = spark.sql(f"SELECT * FROM {db_name}.{table_name}")
    return df

def read_impala_table(config):
    """Reads an Impala table and returns a Pandas DataFrame."""
    conn = connect(host=config.get('impala', 'host'),
                   port=config.getint('impala', 'port'),
                   auth_mechanism=config.get('impala', 'auth_mechanism'))
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {config.get('impala', 'database')}.{config.get('impala', 'table')}")
    results = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=column_names)
    conn.close()
    return df

def load_local_file_to_hive(spark, config):
    """Reads a local CSV file and loads it into a Hive table."""
    df = spark.read.csv(config.get('local_files', 'input_csv'), header=True, inferSchema=True)
    db_name = config.get('hive', 'database')
    table_name = config.get('hive', 'table')

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
            -- Define your table schema here based on the CSV structure
        )
    """)

    df.write.mode("overwrite").saveAsTable(f"{db_name}.{table_name}")

def read_hdfs_file_with_pydoop(config):
    """Reads a CSV file from HDFS using Pydoop and returns a Pandas DataFrame."""
    with hdfs.open(config.get('hdfs', 'input_csv')) as f:
        data = []
        for line in f:
            data.append(line.decode('utf-8').strip().split(','))
    column_names = data[0]
    data = data[1:]
    df = pd.DataFrame(data, columns=column_names)
    return df

def read_local_excel_file(config):
    """Reads a local Excel file using Pandas and returns a DataFrame."""
    df = pd.read_excel(config.get('local_files', 'input_excel'))
    return df

def read_hdfs_excel_file(config):
    """Reads an Excel file from HDFS using Pydoop and Pandas, and returns a DataFrame."""
    with hdfs.open(config.get('hdfs', 'input_excel')) as f:
        excel_data = f.read()
    excel_buffer = BytesIO(excel_data)
    df = pd.read_excel(excel_buffer)
    return df
