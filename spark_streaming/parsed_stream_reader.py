from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

spark = (
    SparkSession.builder
    .appName("order-events-parsed-reader")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
    .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


payload_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType (), True),
])

event_schema = StructType([
    StructField("op_type", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("payload", payload_schema, True),
                         
])

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "order-events")
    .option("startingOffsets", "earliest")
    .load()
)

parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), event_schema).alias("data"))
    .select("data.*")
    .select(
        "op_type", "table_name", "event_timestamp",
        "payload.order_id", "payload.customer_id", "payload.order_status",
        "payload.order_purchase_timestamp", "payload.order_approved_at",
        "payload.order_delivered_carrier_date", "payload.order_delivered_customer_date",
        "payload.order_estimated_delivery_date",
        
        
    )
)

query = (
    parsed_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()