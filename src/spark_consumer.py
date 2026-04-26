import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\dell\AppData\Local\Microsoft\WindowsApps\python3.11.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\dell\AppData\Local\Microsoft\WindowsApps\python3.11.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, split, lower, trim
from pyspark.sql.types import StructType, StructField, StringType

# --- Spark Session ---
spark = SparkSession.builder \
    .appName("SocialMediaStreamProcessor") \
    .config("spark.jars", "file:///C:/spark-jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,"
                          "file:///C:/spark-jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,"
                          "file:///C:/spark-jars/kafka-clients-3.4.1.jar,"
                          "file:///C:/spark-jars/commons-pool2-2.11.1.jar") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.driver.memory", "512m") \
    .config("spark.executor.memory", "512m") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Spark Streaming started. Waiting for data from Kafka...\n")

# --- Schema for incoming JSON posts ---
schema = StructType([
    StructField("id",          StringType(), True),
    StructField("user",        StringType(), True),
    StructField("title",       StringType(), True),
    StructField("text",        StringType(), True),
    StructField("publishedAt", StringType(), True),
])

# --- Read stream from Kafka ---
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "social-posts") \
    .option("startingOffsets", "latest") \
    .load()

# --- Parse JSON ---
posts = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# --- Word count transformation ---
words = posts.select(
    explode(split(lower(trim(col("text"))), " ")).alias("word")
).filter(col("word") != "")

word_count = words.groupBy("word").count().orderBy("count", ascending=False)

# --- Display posts in real time ---
posts_query = posts.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

# --- Display word count in real time ---
wordcount_query = word_count.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

posts_query.awaitTermination()