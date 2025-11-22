from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import current_date, current_timestamp
#from pyspark.sql.functions import os, sys
spark = SparkSession.builder.master("local[2]").appName("Spark_sql with DB").getOrCreate()
sc = spark.sparkContext
#complete_url = 'jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$'
table_name='customer'
driver_program='com.mysql.jdbc.Driver'
#dbdf1=spark.read.jdbc(url=complete_url,table=table_name,properties={'driver':driver_program})
#dbdf1.printSchema()
#dbdf1.show()

hive_df = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\txns").select("_c6", "_c7").toDF("City", "States")
df_read_cols = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\txns")
hive_df.show()
trans_struct = StructType([StructField("Seq_No", StringType(), True),
                           StructField("Cob_Date", StringType(), True),
                           StructField("Acc_No", StringType(), True),
                           StructField("Amount", StringType(), True),
                           StructField("Category", StringType(), True),
                           StructField("Machine_Name", StringType(), True),
                           StructField("City", StringType(), True),
                           StructField("State", StringType(), True),
                           StructField("Pay_Method", StringType(), True),
                           ])

hive_df1 = spark.createDataFrame(df_read_cols.rdd, trans_struct)

Curr_Timestamp = current_timestamp()
print(current_timestamp())
hive_df2 = hive_df1.withColumn("Current_date", current_date()).withColumn("Timestamp", current_timestamp())
hive_df2.show()

hive_df2.coalesce(1).write.mode("overwrite").option("header", True).csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\txns_header_tmp")



