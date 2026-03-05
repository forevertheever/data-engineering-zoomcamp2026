import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://localhost:7077") \
    .appName('read‑parquet') \
    .getOrCreate()

print(f"Spark version: {spark.version}")

# df = spark.read.parquet('yellow_tripdata_2025-11.parquet')
df = spark.read \
    .option("header", "true") \
    .csv('taxi_zonne_lookup.csv')

spark.stop()