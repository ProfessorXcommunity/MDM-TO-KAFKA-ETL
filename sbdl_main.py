from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MDM to Kafka ETL") \
    .getOrCreate()

