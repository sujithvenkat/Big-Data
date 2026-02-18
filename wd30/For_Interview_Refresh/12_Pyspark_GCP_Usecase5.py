from pyspark.sql import SparkSession

spark=SparkSession.builder.master("local[2]").appName("Pyspark_GCP").getOrCreate()

#gcs paths
inp_path="gs://cust-datalake-raw/cust/cust.csv"
out_path="gs://cust-datalake-raw/cust_curated_pq/"

df=spark.read.csv(inp_path, header=True, inferSchema=True)
df.write \
  .format("bigquery") \
  .option("table", "cust_hive_raw.daily_trans") \
  .option("temporaryGcsBucket", "cust-datalake-temp") \
  .mode("append") \
  .save()