import os

from pyspark.sql.functions import broadcast

os.environ['HADOOP_HOME'] = "C:\\winutils"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"

from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local[2]").appName("Pyspark_Deep_Core").getOrCreate()
data1=[(1,"A"),(2,"B"),(3,"C")]
data2=[(1,"X"),(3,"Y"),(4,"Z")]

df1=spark.createDataFrame(data1,["id","val1"])
df2=spark.createDataFrame(data2,["id","val2"])
#df1.show()
#df2.show()

df1.join(df2,on="id", how='inner') # only matched row but gives df1 and df2 columns
df1.join(df2,on="id",how="left")
df1.join(df2,on="id",how="right")
df1.join(df2,on="id",how="full",)
df1.join(df2,on="id", how="left_semi") # only matched row but only gives df1 columns -- no df2 always left
df1.join(df2,on="id",how="left_anti")# only non matched row but only gives df1 columns -- no df2 always left
df1.join(df2,how="cross")  # No condition, no matching on id.Every row of DF1 is paired with every row of DF2.
df1.join(broadcast(df2),on="id",how="right")

from pyspark.sql import functions as F,Window

window_data = [(1, "Alice", "IT", "2024-01-01", 5000),
               (2, "Bob", "IT", "2024-01-01", 5000),  # tie
               (3, "Charlie", "IT", "2024-01-02", 6000),
               (4, "David", "HR", "2024-01-01", 4000),
               (5, "Eva", "HR", "2024-01-02", 4500),
               (6, "Frank", "HR", "2024-01-02", 4500),  # tie
               (7, "Grace", "HR", "2024-01-03", 4800),
               ]

header = ["emp_id", "name", "dept", "date", "salary"]
df = spark.createDataFrame(window_data,header)
#df.show()

#row_number – Latest record per department
w=Window.partitionBy("dept").orderBy(F.desc("date"))
df.withColumn("row_num", F.row_number().over(w)).filter("row_num = 1")
w= Window.partitionBy("dept").orderBy(F.desc("date"))
df.withColumn("row_num", F.row_number().over(w)).filter()
#rank vs dense_rank – Salary ranking
w2=Window.partitionBy("dept").orderBy(F.desc("salary"))
df.withColumn("rnk",F.rank().over(w2)).withColumn("dense_rnk", F.dense_rank().over(w2))
#lag / lead – Salary change over time
w3=Window.partitionBy("dept").orderBy("date")
df.withColumn("prev_day_sal", F.lag("salary").over(w3)).withColumn("next_day_sal",F.lead("salary").over(w3))
#Running total – Dept salary over time
w4=Window.partitionBy("dept").orderBy("date").rowsBetween(Window.unboundedPreceding,Window.currentRow)
df.withColumn("running_total",F.sum("salary").over(w4))
#Moving average – 3-day rolling salary
w5=Window.partitionBy("dept").orderBy("date").rowsBetween(-2,0)
df.withColumn("moving_avg", F.avg("salary").over(w5))

w6=Window.partitionBy("dept").orderBy("date")
df.withColumn("total_sum",F.sum("salary").over(w6))

'''For w6, Spark uses this default frame:
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
👉 That is why you are getting a running total, not a full dept total 
and  Why does Eva show 13000 instead of 8500?
Because of ROWS vs RANGE behavior:
You used orderBy("date")
Eva and Frank have the same date
Spark groups same-order values together
So for date = 2024-01-02:
👉 Spark includes ALL rows with that date'''


aggregation_data = [
    ("O1", "C1", "Electronics", "2024-01-01", 1000),
    ("O2", "C1", "Electronics", "2024-01-02", 1500),
    ("O3", "C2", "Electronics", "2024-01-01", 2000),
    ("O4", "C2", "Clothing", "2024-01-02", 500),
    ("O5", "C3", "Clothing", "2024-01-02", 700),
    ("O6", "C1", "Clothing", "2024-01-03", 300),
    ("O7", "C1", "Electronics", "2024-01-03", 1200),
]
agg_header=["order_id", "customer_id", "category", "order_date", "amount"]
df=spark.createDataFrame(aggregation_data,agg_header)


df.groupBy("category","order_date").agg(
    F.sum("amount").alias("total_sum")
)

df.groupBy("category").agg(
    F.collect_list("customer_id").alias("customer list"),
    F.collect_set("customer_id").alias("Unique_customers"),
    F.avg("amount").alias("Avg amount per category"),
    F.count("*").alias("count per category")
)

array_struct_data = [
    ("01", ["Light", "Mouse"], [1000, 50]),
    ("02", ["Shirt", "Jeans"], [500, 700]),
    ("O3", ["Phone"],                 [800]),
    ("O4", ["Shoes", "Socks", "Cap"], [1200, 100, 200])
]

df=spark.createDataFrame(array_struct_data, ["order_id", "items", "prices"])
df.show(truncate=False)

df.select("order_id", F.explode("items").alias("item"))
#df.select("order_id", F.explode("items").alias("item"), F.explode("prices").alias("price"))
df.select("order_id", F.arrays_zip("items","prices")).alias("zipped")
df.withColumn("zipped", F.arrays_zip("items","prices"))
df.withColumn("zipped", F.arrays_zip("items","prices")).\
    select("order_id", F.explode("zipped").alias("unzip")).\
    select("order_id",
           F.col("unzip.items").alias("item"),
           F.col("unzip.prices").alias("price")
           )
df.withColumn("discounted_price", F.expr("transform(prices, p -> p*0.9)"))
df.withColumn("expensive_prices", F.expr("filter(prices, p -> p > 500)"))
df.withColumn("item_price_map", F.map_from_entries(F.arrays_zip("items", "prices")))
df.withColumn("order_summary", F.struct(
    "order_id",
    F.size("items").alias("item_count"),
    F.expr("aggregate(prices, CAST(0 AS BIGINT), (acc, x) ->  acc + x)").alias("total_amount")
)).show(truncate=False)