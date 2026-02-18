import os
import gcsfs
import pandas as pd

os.environ['HADOOP_HOME'] = "C:\\winutils"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"E:\Downloads\service-account.json"

from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local[2]").appName("Pyspark_GCP_Usecase1").\
    config("spark.jars",r"E:\Downloads\gcs-connector-3.0.0.jar").\
    getOrCreate()

gcs_path="gs://cust-datalake-raw/cust/cust.csv"
fs = gcsfs.GCSFileSystem()
with fs.open(gcs_path) as f:
    pdf = pd.read_csv(f)

# Convert to Spark DataFrame
df = spark.createDataFrame(pdf)
df.createOrReplaceTempView("cust_hive_table")
spark.sql("select * from cust_hive_table")
#df=spark.read.csv(gcs_path,header=True,inferSchema=True)
df.show(5)

df2=spark.read.csv("E:\Bigdata\Shared_Documents\sample_data\hive\data\custinfo_cst.csv", header=True, inferSchema=True)
#df2.show()
pdf = df2.toPandas()
#with fs.open("gs://cust-datalake-raw/daily_trans/daily_trans.csv", 'w') as f:
#    pdf.to_csv(f, index=False)

print("Daily transaction data loaded from HDFS (local) → GCS")

