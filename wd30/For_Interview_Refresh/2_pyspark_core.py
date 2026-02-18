import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import *
os.environ['HADOOP_HOME'] = "C:\\winutils"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
spark=SparkSession.builder.appName("class_workouts").master("local[*]").enableHiveSupport().getOrCreate()
# print(spark)
# print(spark.sparkContext.version)
# print(spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
# df1=spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\cust.txt")
# df1_add_header=df1.toDF("custid","city","product","amt")
# df1_select=df1_add_header.select("city")
# df1.write.mode("overwrite").json("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\write\\json\\")

data = [
("A", 1), ("A", 2), ("A", 3), ("A", 4),
("B", 1),
("C", 1)
]
columns = ["id", "name", "city"]

old_data = [
(1, "Sujith", "Chennai"),
(2, "Arun", "Bangalore")
]
new_data = [
(1, "Sujith", "Hyderabad")
]

df1=spark.createDataFrame(old_data,columns)
df2=spark.createDataFrame(new_data,columns)
df1.show()
df2.show()

#df2=df1.union(df2)
#df2.withColumn("version", F.row_number().over((Window.partitionBy("id"))))

df2.alias("n").join(df1.alias("o"),on="id", how="left").select("id",
                                                               coalesce("n.name","o.name"),
                                                               coalesce("n.city","o.name")
                                         ).show()




