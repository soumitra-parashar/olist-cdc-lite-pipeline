from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("test-connection")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

print(f"Spark version: {spark.version}")
print(f"Scala version: {spark.sparkContext._jvm.scala.util.Properties.versionString()}")

spark.stop()