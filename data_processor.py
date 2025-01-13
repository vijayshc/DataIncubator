import configparser
from utils import get_spark_session, read_hive_with_spark, read_hive_with_hwc, read_impala_table, \
    load_local_file_to_hive, read_hdfs_file_with_pydoop, read_local_excel_file, read_hdfs_excel_file

def main():
    # Load configuration
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Get processing method from config
    processing_method = config.get('processing', 'method')

    # Create SparkSession if needed
    if processing_method in ['read_hive_spark', 'read_hive_hwc', 'load_local_to_hive']:
        spark = get_spark_session(config)
    else:
        spark = None

    # Perform the selected operation
    if processing_method == 'read_hive_spark':
        df = read_hive_with_spark(spark, config)
        df.show()
    elif processing_method == 'read_hive_hwc':
        df = read_hive_with_hwc(spark, config)
        df.show()
    elif processing_method == 'read_impala':
        df = read_impala_table(config)
        print(df)
    elif processing_method == 'load_local_to_hive':
        load_local_file_to_hive(spark, config)
    elif processing_method == 'read_hdfs_pydoop':
        df = read_hdfs_file_with_pydoop(config)
        print(df)
    elif processing_method == 'read_local_excel':
        df = read_local_excel_file(config)
        print(df)
    elif processing_method == 'read_hdfs_excel':
        df = read_hdfs_excel_file(config)
        print(df)
    else:
        print(f"Invalid processing method: {processing_method}")

    # Stop SparkSession if created
    if spark:
        spark.stop()

if __name__ == "__main__":
    main()
