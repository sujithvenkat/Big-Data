import sys
from pyspark.sql import SparkSession
# from pyspark.sql import *
import os
# os.environ["HADOOP_HOME"]="C:/winutils/"
spark=SparkSession.builder.master("local").appName("sample1").getOrCreate()
df1 = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\empdata.txt")


def main(args):
    print("inside main")
    df1 = spark.read.csv(args)
    df1.toDF("Name", "City", "Age", "DOJ", "Salary").show(10)


if __name__ == "__main__":
    print(sys.argv)
    main("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\empdata.txt")





