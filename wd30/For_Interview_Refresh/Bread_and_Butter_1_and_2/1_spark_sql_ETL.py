import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql import readwriter
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType
os.environ['HADOOP_HOME'] = "C:\\winutils"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
spark=SparkSession.builder.appName("Bread and Butter").master("local[2]").enableHiveSupport().getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("ERROR")
#Creating Dataframe from RDD
rdd1=sc.textFile("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\sampledata")
splitted_rdd1=rdd1.map(lambda x:x.split(','))
schema_rdd=splitted_rdd1.map(lambda x:(int(x[0]),x[1]))
col_list=['Id','Name']
df=schema_rdd.toDF(col_list)
#df.show()
#df.printSchema()
#print(prints.collect())
schema=StructType([StructField("Id",IntegerType(),False),
                  StructField("Name", StringType(), False)])
df2=spark.read.schema(schema).csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\sampledata")
#df2.show()
df2.select("*").where("id > 2")

defile_df1_default=spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")
defile_df1_tilt=spark.read.option("delimiter","~").csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")
#defile_df1_tilt.show()
#defile_df1_tilt.printSchema()
defile_df1_header=spark.read.option("delimiter","~").option("inferschema", True).option("header", True).csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")
#defile_df1_header.show()
#defile_df1_header.printSchema()
defile_df1=spark.read.options(header = True,inferSchema = True, delimiter = '~').csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse_header.csv")
defile_df1.toDF("Exchange_Rate","Stock", "Close_Rate")
defile_df1.show()
defile_df1.withColumnRenamed("exchange","Exchange_Rate").withColumnRenamed("close", "Close_Rate")
defile_df2=defile_df1.select(col("exchange").alias("Exchange_Rate"),"stock_name",col("close").alias("Close_Rate"))
defile_df2.createOrReplaceTempView("view_one")
spark.sql("select * from view_one where Exchange_Rate = 'BSE' ").show()
defile_df2.groupBy("Exchange_Rate").agg(F.max(col("Close_Rate")).alias("max_close_rate"),F.min(col("Close_Rate")).alias("min_close_rate"))

columns=[
    ("id",IntegerType()),
    ("name", StringType()),
    ("Age", IntegerType()),
    ("Salary", FloatType())
]

schema_colums=StructType([
    StructField(col_name, col_type, True) for col_name, col_type in columns
])
print(schema_colums)
# rdd1=sc.textFile("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\tild_separate_file1")
# print(rdd1.take(5))
# split_rdd1=rdd1.map(lambda x:x.split('~'))
# print(split_rdd1.take(5))
# each_col_rdd1=split_rdd1.map(lambda x:[x[0],x[1],x[2]])
# print(each_col_rdd1.take(5))
# flat_map_Rdd=rdd1.flatMap(lambda x:x.split('~'))
# print(flat_map_Rdd.take(10))
# flat_map_Rdd1=flat_map_Rdd.flatMap(lambda x:x[0])
# print(flat_map_Rdd1.take(5))
