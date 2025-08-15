from pyspark.sql import SparkSession

print("🔥 Creating Spark session")
spark = (
    SparkSession.builder
    .appName("VenvSparkSessionTest")
    .getOrCreate()
)

print("✅ Spark Session Created!")
print("🚀 Spark Version:", spark.version)
