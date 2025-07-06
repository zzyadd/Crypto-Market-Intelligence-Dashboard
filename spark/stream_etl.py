from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Define schema
schema = StructType() \
    .add("id", StringType()) \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("volume", DoubleType()) \
    .add("timestamp", StringType())

# Initialize Spark
spark = SparkSession.builder \
    .appName("CryptoKafkaETL") \
    .getOrCreate()

# Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto-stream") \
    .load()

# Parse JSON value
json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Write to PostgreSQL
query = json_df.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda batch_df, _: batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://db:5432/crypto") \
        .option("dbtable", "prices") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()) \
    .start()

query.awaitTermination()
