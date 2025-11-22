import os

from pyspark.sql.functions import col, min, max, sum, desc
from pyspark.sql import functions as F
os.environ['HADOOP_HOME'] = "C:\\winutils"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
input_path="E:/BigData/Shared_Documents/sample_data/sparkdata/"
inpfile=os.path.join(input_path, "Dept.txt")
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, TimestampType
spark=SparkSession.builder.appName("Pyspark DSL").master("local[*]").getOrCreate()
#df=spark.read.option("header", True).csv(inpfile, sep='\t')
data = [
    (1, "E1", "Fin", 1000, "2023-01-10", "M", ["Math","Physics"], "big data spark spark hadoop"),
    (2, "E2", "Fin", 2000, "2023-01-12", "F", ["Math","Chemistry"], "spark hadoop python java"),
    (3, "E3", "Fin", 3000, "2023-02-01", "M", ["Physics","Chemistry"], "big data hadoop hive"),
    (4, "E4", "Admin", 1000, "2023-01-15", "F", ["Excel","HR"], "airflow dataflow spark"),
    (5, "E5", "Admin", 1500, "2023-02-05", "M", ["Python","HR"], "python spark java"),
    (6, "E6", "Admin", 1800, "2023-02-20", "F", ["Excel","Python"], "hive spark hadoop"),
    (7, "E7", "HR", 500, "2023-01-22", "F", ["HR","Communication"], "communication hr python"),
    (8, "E8", "HR", 600, "2023-02-12", "M", ["HR","Management"], "management hr excel"),
    (9, "E9", "HR", 800, "2023-03-01", "F", ["Management","Excel"], "hr excel dataflow"),
]

schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("EmpID", StringType(), True),
    StructField("Dept", StringType(), True),
    StructField("Sal", IntegerType(), True),
    StructField("JoinDate", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("subjects", ArrayType(StringType()), True),
    StructField("Sentence", StringType(), True)
])

df = spark.createDataFrame(data, schema)
df.show()
df1=df.groupBy("Dept").agg(max(col("Sal")).alias("max_sal"),min("Sal")).orderBy("Dept")
#Q1. Find the department with maximum total salary
df2=df.groupBy("Dept").agg(sum(col("Sal")).alias("Total_Sal"))
w= Window.orderBy(F.desc("total_sal"))
result = df2.withColumn("rn", F.rank().over(w)).filter("rn = 1").drop("rn")
#result.show()

#Remove duplicates but keep highest salary per employee
df3=df.groupBy("EmpID").agg(F.max("Sal")).orderBy(F.desc("max(Sal)"))
    #.orderBy(F.desc)
w= Window.partitionBy("Dept").orderBy(F.desc("Sal"))
df3=df.withColumn("rn", F.row_number().over(w))
df3.show()


