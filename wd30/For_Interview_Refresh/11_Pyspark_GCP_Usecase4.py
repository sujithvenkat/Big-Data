from pyspark.sql import SparkSession

spark=SparkSession.builder.master("local[2]").appName("Pyspark_GCP").getOrCreate()

#gcs paths
inp_path="gs://cust-datalake-raw/daily_trans/daily_trans.csv"
out_path="gs://cust-datalake-raw/daily_trans_curated/"

df=spark.read.csv(inp_path, header=True, inferSchema=True)

df_clean=df.filter("cust_id != 2")

df_clean.write.mode("overwrite").option("header", True).csv(out_path)
df_clean.show(5)

spark.stop()
