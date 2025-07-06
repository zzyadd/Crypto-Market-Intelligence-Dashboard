from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import logging

schema = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

spark = SparkSession.builder \
    .appName("CryptoKafkaETL") \
    .config("spark.jars", "/opt/postgresql-42.2.18.jar") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto-stream") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

def write_to_postgres(batch_df, batch_id):
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://db:5432/crypto") \
            .option("dbtable", "prices") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("batchsize", 1000) \
            .option("isolationLevel", "NONE") \
            .mode("append") \
            .save()
        print(f"Batch {batch_id} written successfully.")
    except Exception as e:
        logging.error(f"Error writing batch {batch_id} to Postgres: {e}")

query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
