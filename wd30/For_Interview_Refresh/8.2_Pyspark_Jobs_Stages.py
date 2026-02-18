import os
os.environ['HADOOP_HOME'] = "C:\\winutils"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Spark-UI-Demo")
    .master("local[*]")
    .getOrCreate()
)

df = spark.range(0, 1000000)
df = df.repartition(4)
df.groupBy().count().show()

input("Press Enter to stop Spark...")
spark.stop()
