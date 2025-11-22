import os

from pyspark.sql.functions import lower, col, upper
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ShortType

os.environ['HADOOP_HOME'] = "C:\\winutils"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
from delta import *
from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local[*]").appName("Bread and Butter2").getOrCreate()
fil_loc="E:/BigData/Shared_Documents/sample_data/hive/data"
file1=os.path.join(fil_loc, "custsmodified")
custdf1=spark.read.csv(file1,mode='permissive', inferSchema=True)
print(custdf1.count())
#custdf1.printSchema()
custdf1.show(10, False)

#custdf1.where(upper(col("_c0")) != lower(col("_c0"))).show() #to identify non numeric data as the c0 datattype is string
#custdf1.filter(col("_c0").isNull()).show() #to identify the column is null
#print(to_print)
struct_schema=StructType([StructField("Id", IntegerType(), True),StructField("custfname", StringType(), False),StructField("custlname", StringType(), True),
 StructField("custage", ShortType(), True),StructField("custprofession", StringType(), True)])
cust_clean_df1=spark.read.csv(file1,mode='dropmalformed',schema=struct_schema)
cust_clean_df1.where("Id is null").show()
cust_clean_df1.where("upper(id) <> lower(id)").show()
cust_clean_df1.dropDuplicates().na.drop("any").count()
cust_clean_df1.dropDuplicates().na.drop("all").count()
cust_clean_df2=cust_clean_df1.na.fill("na",subset=["custprofession"])
prof_dict={"Therapist":"Physician","Musician":"Music Director","na":"not defined"}
cust_scrub=cust_clean_df2.na.replace(prof_dict,subset=["custprofession"])
#print(len(custdf1.collect()))
cust_scrub_renamed=cust_scrub.withColumnRenamed("_c0","Id")
custdf1.join(cust_scrub_renamed,custdf1.Id == cust_scrub_renamed.Id, how="left").show(5)