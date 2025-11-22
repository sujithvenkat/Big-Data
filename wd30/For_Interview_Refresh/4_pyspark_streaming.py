import os
os.environ['HADOOP_HOME'] = "C:\\winutils"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
input_path="E:/BigData/Shared_Documents/sample_data/streaming/input/"
output_path = "E:/BigData/Shared_Documents/sample_data/streaming/output_parquet/"
checkpoint_path= "E:/BigData/Shared_Documents/sample_data/streaming/chkpt/"
inpfile=os.path.join(input_path, "test1.json")
spark=SparkSession.builder.appName("Structured Streaming").master("local[*]").getOrCreate()
schema=StructType([
    StructField("order_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("event_time", TimestampType())
])
raw_df=spark.readStream.format("json").schema(schema).load(input_path)
raw_df=raw_df.withWatermark("event_time", "10 minutes").dropDuplicates(["order_id"])
query=raw_df.writeStream.format("parquet").\
    option("truncate", False).\
    option("path",output_path).\
    option("checkpointLocation", checkpoint_path).\
    outputMode("append").\
    partitionBy("event_time").\
    trigger(processingTime="30 seconds").\
    start()

query.awaitTermination()



#Spark must know a cutoff time after which it can safely finalize a window. That cutoff is the watermark.