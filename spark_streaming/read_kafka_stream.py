from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("orders-events-reader")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders-events")
    .option("startingOffsets", "earliest")
    .load()
)

readable_df = df.selectExpr(
    "CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp"
    
)

query = (
    readable_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()