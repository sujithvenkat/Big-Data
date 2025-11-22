#import findspark
#findspark.init()
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("tst").getOrCreate()

df = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv",sep="~", header=True, inferSchema=True)
print(df.rdd.count())


