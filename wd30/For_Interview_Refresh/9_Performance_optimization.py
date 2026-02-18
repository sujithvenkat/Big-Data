import os

from pyspark.sql import functions as F

os.environ['HADOOP_HOME'] = "C:\\winutils"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"

from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local[2]").appName("Performance_optimization").getOrCreate()

df= (spark.range(0, 200_000_000).
     withColumn("order_date", F.expr("date_add('2024-01-01', CAST(id % 30 AS INT))")).
     withColumn("country", F.when(F.col("id")% 5 == 0, "IN").otherwise("US")).
     withColumn("amount", F.col("id") % 10000)
     )
df.write.partitionBy("order_date").parquet("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\write\\large_data\\")