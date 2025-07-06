from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define the JSON schema matching Kafka producer
schema = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Create Spark session with JDBC driver for PostgreSQL
spark = SparkSession.builder \
    .appName("CryptoKafkaETL") \
    .config("spark.jars", "/opt/postgresql-42.2.18.jar") \
    .getOrCreate()
    # Ensure this JAR is available

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto-stream") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON messages
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write the parsed stream to PostgreSQL in micro-batches
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://db:5432/crypto") \
        .option("dbtable", "prices") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append") \
        .save()

# Stream with foreachBatch logic
query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
