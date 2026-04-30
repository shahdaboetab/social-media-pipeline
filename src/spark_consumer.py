import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\dell\AppData\Local\Microsoft\WindowsApps\python3.11.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\dell\AppData\Local\Microsoft\WindowsApps\python3.11.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, split, lower, trim, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from textblob import TextBlob

# ---------------------------------
# 1. Spark Session
# ---------------------------------
spark = SparkSession.builder \
    .appName("SocialMediaStreamProcessor") \
    .config("spark.jars", "file:///C:/spark-jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,"
                          "file:///C:/spark-jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,"
                          "file:///C:/spark-jars/kafka-clients-3.4.1.jar,"
                          "file:///C:/spark-jars/commons-pool2-2.11.1.jar,"
                          "file:///C:/spark-jars/mssql-jdbc-12.4.2.jre11.jar") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.driver.memory", "512m") \
    .config("spark.executor.memory", "512m") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark Streaming started...")

# ---------------------------------
# 2. Schema
# ---------------------------------
schema = StructType([
    StructField("id",          StringType(), True),
    StructField("user",        StringType(), True),
    StructField("title",       StringType(), True),
    StructField("text",        StringType(), True),
    StructField("publishedAt", StringType(), True),
])

# ---------------------------------
# 3. Read from Kafka
# ---------------------------------
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "social-posts") \
    .option("startingOffsets", "latest") \
    .load()

posts = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# ---------------------------------
# 4. Sentiment Analysis
# ---------------------------------
def get_sentiment(text):
    try:
        return float(TextBlob(text).sentiment.polarity)
    except:
        return 0.0

sentiment_udf = udf(get_sentiment, FloatType())
posts_sentiment = posts.withColumn("sentiment", sentiment_udf(col("text")))

# ---------------------------------
# 5. Word Count
# ---------------------------------
words = posts.select(
    explode(split(lower(trim(col("text"))), " ")).alias("word")
).filter(col("word") != "")

word_count = words.groupBy("word").count()

# ---------------------------------
# 6. Save to SQL Server (foreachBatch)
# ---------------------------------
def save_to_sqlserver(df, epoch_id):
    if df.isEmpty():
        return
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:sqlserver://localhost:1433;"
                       "databaseName=socialdb;"
                       "encrypt=false;"
                       "trustServerCertificate=true;") \
        .option("dbtable", "dbo.posts") \
        .option("user", "sa") \
        .option("password", "Tokyo@907") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .mode("append") \
        .save()

sqlserver_query = posts_sentiment.writeStream \
    .foreachBatch(save_to_sqlserver) \
    .outputMode("append") \
    .option("checkpointLocation", "file:///C:/spark-checkpoints/sqlserver/") \
    .start()

# ---------------------------------
# 7. Send word count to Kafka
# ---------------------------------
word_to_kafka = word_count.selectExpr("to_json(struct(*)) AS value")

wordcount_query = word_to_kafka.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "wordcount-stream") \
    .outputMode("complete") \
    .option("checkpointLocation", "file:///C:/spark-checkpoints/wordcount/") \
    .start()

# ---------------------------------
# 8. Console Output
# ---------------------------------
console_query = posts_sentiment.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .option("checkpointLocation", "file:///C:/spark-checkpoints/console/") \
    .start()

console_query.awaitTermination()