import os
from pyspark.sql import SparkSession
os.environ['HADOOP_HOME'] = "C:\\winutils"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
spark=SparkSession.builder.appName("class_workouts").master("local[*]").enableHiveSupport().getOrCreate()
print(spark)
print(spark.sparkContext.version)
print(spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
df1=spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\cust.txt")
df1_add_header=df1.toDF("custid","city","product","amt")
df1_select=df1_add_header.select("city")
df1.write.mode("overwrite").json("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\write\\json\\")
#df1_select.show()
