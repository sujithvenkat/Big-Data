import os

from pyspark.sql.functions import col, min, max, sum, desc
from pyspark.sql import functions as F
os.environ['HADOOP_HOME'] = "C:\\winutils"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
input_path="E:/BigData/Shared_Documents/sample_data/sparkdata/venky_sample_inp_files/"
input_path1="E:/BigData/Shared_Documents/sample_data/sparkdata/nyse/"
out_path="E:/Bigdata/Shared_Documents/sample_data/sparkdata/write/dsl_workouts_results/"
inpfile=os.path.join(input_path, "Dept.txt")
csvfile=os.path.join(input_path1,"anyse1.csv")
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, TimestampType, FloatType
from pyspark.sql import functions as F
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
#df.show()
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
#df3.show()
schema=StructType([
    StructField("Exchange", StringType(),True),
    StructField("Mode", StringType(),True),
    StructField("Price", StringType(),True),
    StructField("Zone", StringType(), True)
])
df_csv=spark.read.csv(csvfile, sep='~',schema=schema)
df_dropped_null=df_csv.dropna().withColumn("Price",col("Price").cast("int"))
#f_dropped_null.printSchema()
#df_dropped_null.show()
df_dropped_null.coalesce(1).write.mode("overwrite").parquet(out_path + "nyse")

#From sales data, find the top 3 products by sale_amount per category.
df_sales=spark.read.csv(input_path + "sales2.csv", header=True)
top_values=df_sales.groupBy("category").agg(F.sort_array(F.collect_list("sale_amount")).alias("amounts")).\
    withColumn("top_values",F.slice(F.reverse("amounts"),1,3))
#top_values.show(20,False)

w=Window.partitionBy("category").orderBy(desc("sale_amount"))
df_sales.withColumn("row_num",F.row_number().over(w)).where("row_num < 4")

#| name | month | amount | Into:   | name | Jan | Feb | Mar |

df_monthly=spark.read.csv(input_path + "monthly.csv", header=True)
#df_monthly.show()
df_monthly.groupBy("name").pivot("month",["Jan","Feb","Mar"]).agg(F.sum("amount")).na.fill(0)
df_monthly.groupBy("name").agg(F.expr("sum(case when month = 'Jan' then amount else 0 end)").alias("Jan"),
                               F.expr("sum(case when month = 'Feb' then amount else 0 end)").alias("Feb"),
                               F.expr("sum(case when month = 'Mar' then amount else 0 end)").alias("Mar"))
#df_sales.show()
#Given a nested JSON column named info, flatten it.
df_json=spark.read.json(input_path + "nested.json")
#df_json.show(10, False)
df_json.withColumn("item",F.explode("items")).select("id","name","item.product","item.price")

#Remove duplicate customer rows by keeping the latest updated_at record.
df_cust=spark.read.csv(input_path + "customer5.csv", header=True)
#df_cust.show()
w=Window.partitionBy("cust_id").orderBy(desc("updated_at"))
df_cust.withColumn("row_num",F.row_number().over(w)).where("row_num == 1").select("cust_id","name","updated_at")
#Perform Type-1 upsert between source and target datasets.

df_fact=spark.read.csv(input_path + "fact.csv", header=True)
df_dim=spark.read.csv(input_path + "dimension.csv", header=True)
#df_fact.show()
#df_dim.show()
df_fact.join(F.broadcast(df_dim),how="left",on="id")

#spark.conf.set("spark.sql.shuffle.partitions", 100)


