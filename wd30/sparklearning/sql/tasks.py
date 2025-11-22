from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, sum, when, avg, mean, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import current_date, current_timestamp
#from pyspark.sql.functions import os, sys
spark = SparkSession.builder.master("local[2]").appName("Spark_sql with DB").getOrCreate()

'''Team,
Uploaded the today's code, pls try to work on the usecase..
#Quick Usecase:
#Write a python def function to calculate the age grouping/categorization of the people based on the custage column, if age<13 - childrens, if 13 to 18 - teen, above 18 - adults
#derive a new column called age group in the above dataframe (using DSL)
#Try the same with the SQL also'''

cust_dt = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\custdata.txt")
cust_dt1 = spark.createDataFrame(cust_dt.rdd,["name","place","age","zipcode"])
cust_dt1.show()

cust_dt1.withColumn("age_group", when((col("age").cast("int") < 35), lit("below_35_range")).otherwise(lit("Above_35_range"))).show()

'''
Team,
Uploaded the uptodate code and the hive/data also, pls make use of...
Convert the sql in line 712 to SQL as given below...
#SQL Equivalent of the Same
#convert it...
#keep groupby and agg() in the select
#keep the first where clause in the where clause
#keep groupby in the groupby
#keep last where clause in the having
#keep orderby in the last
'''

dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_consumption2=\
    custom_agegrp_munged_enriched_df.where("agegroup<>'Children'")\
        .groupby("year","agegroup","custprofession")\
        .agg(max("curdt").alias("max_curdt"),
             min("curdt").alias("min_curdt"),
             avg("custage").alias("avg_custage"),
             mean("custage").alias("mean_age"),
             countDistinct("custage").alias("distinct_cnt_age"))\
        .orderBy("year","agegroup","custprofession",ascending=[False,True,False])

spark.sql("select year, agegroup, custprofession, max(curdt) max_curdt, min(curdt) min_curdt, avg(custage) avg_custage, mean(custage) mean_age, "
          "countDistinct(custage) distinct_cnt_age from custom_agegrp_munged_enriched_df where agegroup <>'Children' "
          " group by year, agegroup, custprofession order by year desc, agegroup, custprofession desc")



