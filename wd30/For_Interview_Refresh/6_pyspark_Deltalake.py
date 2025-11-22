import os

from pyspark.sql.functions import col, min, max, sum, desc
from pyspark.sql import functions as F

os.environ['HADOOP_HOME'] = "C:\\winutils"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
input_path="E:/BigData/Shared_Documents/sample_data/sparkdata/"
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, TimestampType
from delta.
spark=SparkSession.builder.appName("DeltaLake").\
    config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").\
    config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").\
    master("local[*]").getOrCreate()
data = [
    (1, "E1", "Fin", 1000),
    (2, "E2", "Fin", 2000),
    (3, "E3", "Admin", 1500)
]

schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("EmpID", StringType(), True),
    StructField("Dept", StringType(), True),
    StructField("Sal", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)

df.write.format("delta").mode("overwrite").save("E:/BigData/Shared_Documents/sample_data/sparkdata/delta/events")

delta_table = DeltaTable