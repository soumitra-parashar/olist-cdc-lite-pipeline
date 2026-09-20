import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, from_json, to_timestamp, last, row_number
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip


builder = (
    SparkSession.builder
    .appName("order-events")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"]
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DELTA_PATH = os.path.join(REPO_ROOT, "delta", "orders_silver")
CHECKPOINT_PATH = os.path.join(REPO_ROOT, "checkpoints", "order_merge")

UPDATE_COLS = [
    "customer_id", "order_status", "order_purchase_timestamp",
    "order_approved_at", "order_delivered_carrier_date",
    "order_delivered_customer_date", "order_estimated_delivery_date",
]

payload_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True),
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

def dedupe_batch(batch_df):
    
    df = batch_df.withColumn("event_ts", to_timestamp("event_timestamp"))
    
    fill_window = (
        Window.partitionBy("order_id")
        .orderBy("event_ts")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        
    )
    
    for c in UPDATE_COLS:
        df = df.withColumn(c, last(col(c), ignorenulls=True).over(fill_window))
            
    latest_window = Window.partitionBy("order_id").orderBy(col("event_ts").desc())
    df = (
        df.withColumn("rn", row_number().over(latest_window))
        .filter(col("rn") == 1)
        .drop("rn", "event_ts")
    )
    return df
    
def upsert_to_delta(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id}: empty, skipping")
        return
    
    deduped = dedupe_batch(batch_df)
    
    if not DeltaTable.isDeltaTable(spark, DELTA_PATH):
        print(f"Batch {batch_id}: creating Delta table at {DELTA_PATH}")
        deduped.write.format("delta").mode("overwrite").save(DELTA_PATH)
        return

    target = DeltaTable.forPath(spark, DELTA_PATH)
    update_set = {c: f"COALESCE(s.{c}, t.{c})" for c in UPDATE_COLS}
    
    (
        target.alias("t")
        .merge(deduped.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    print(f"Batch {batch_id}: merged {deduped.count()} order(s)")

query = (
    parsed_df.writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

query.awaitTermination()