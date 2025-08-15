import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.main.utility.logging_config import *

def spark_session():
    spark = SparkSession.builder.master("local[*]") \
        .appName("rinkal_spark2")\
        .config("spark.driver.extraClassPath", "file:///C:/spark/spark-3.5.5-bin-hadoop3/jars/mysql-connector-j-9.4.0.jar") \
        .getOrCreate()
    logger.info("spark session %s",spark)
    return spark
# .config("spark.jars", "file:///C:/spark/spark-3.5.5-bin-hadoop3/jars/mysql-connector-j-9.4.0.jar") \


