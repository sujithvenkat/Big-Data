from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
from dateutil import relativedelta

import sys
import os
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("Hackathon_Usecase_1").getOrCreate()
location = "E:\\BigData\\Shared_Documents\\sample_data\\sparkhack2\\"


schema = StructType([StructField("results", ArrayType(StructType([StructField("gender", StringType()),
                                                                  StructField("location",StructType([StructField("city", StringType()),
                                                                                                     StructField("state", StringType()),
                                                                                                     StructField("country", StringType()),
                                                                                                     StructField("postcode", StringType()),
                                                                                                     StructField("coordinates",StructType([StructField("latitude", StringType()),
                                                                                                                                           StructField("longitude", StringType())])),])),
                                                                  StructField("name", StructType([StructField("title",StringType()),
                                                                                                  StructField("first", StringType()),
                                                                                                  StructField("last", StringType())])),
                                                                  StructField("email", StringType()),
                                                                  StructField("login", StructType([StructField("uuid", StringType()),
                                                                                                   StructField("username", StringType()),
                                                                                                   StructField("password", StringType())])),
                                                                  StructField("dob", StructType([StructField("date", StringType()),
                                                                                                 StructField("age", StringType())])),
                                                                  StructField("registered", StructType([StructField("date", StringType()),
                                                                                                        StructField("age", StringType())])),
                                                                  StructField("phone",StringType()),
                                                                  StructField("cell",StringType())]
                                                                 )
                                                      )
                                 )
                     ]
                    )
json_df = spark.read.json(location + 'apidata.json', multiLine=True, schema=schema)
#json_df.show(5, False)
json_df.printSchema()
exploded_df = json_df.selectExpr('explode(results) as exploded_cols')
#exploded_df.show(5, False)

flattened_df = exploded_df \
    .withColumn('gender', col('exploded_cols.gender')) \
    .withColumn('city', col('exploded_cols.location.city')) \
    .withColumn('state', col('exploded_cols.location.state')) \
    .withColumn('title', col('exploded_cols.name.title')) \
    .withColumn('first', col('exploded_cols.name.first')) \
    .withColumn('last', col('exploded_cols.name.last')) \
    .withColumn('latitude', col('exploded_cols.location.coordinates.latitude')) \
    .withColumn('longitude', col('exploded_cols.location.coordinates.longitude')) \
    .withColumn('email', col('exploded_cols.email')) \
    .withColumn('uuid', col('exploded_cols.login.uuid')) \
    .withColumn('username', col('exploded_cols.login.username')) \
    .withColumn('password', col('exploded_cols.login.password')) \
    .withColumn('dob_dt', col('exploded_cols.dob.date')) \
    .withColumn('dob_age', col('exploded_cols.dob.age')) \
    .withColumn('login_dt', col('exploded_cols.registered.date')) \
    .withColumn('login_age', col('exploded_cols.registered.age')) \
    .withColumn('phone', col('exploded_cols.phone')) \
    .withColumn('cell', col('exploded_cols.cell')) \
    .drop('exploded_cols')





flattened_df.show(5, False)

print("\n5. Identify the below results out of it using DSL and SQL")
print("a. Identify the gender wise count")
flattened_df.groupby("gender").agg(count(col("*")).alias("gender_wise_count")).show()

print("b.Identify the gender wise average age")
flattened_df.groupby("gender").agg(count(col("*")).alias("gender_wise_count"), avg(col("dob_age")).alias("avg_age")).show()

print("c. Extract only the date portion of the dob column")
#in stringtype
flattened_df.withColumn("dob_dt_string", substring("dob_dt", 0, 10))
#in datetype
flattened_date_df = flattened_df.withColumn("dob_dt_string", substring("dob_dt", 0, 10)).withColumn("dob_dt_date", to_date(col("dob_dt").cast("timestamp")))
flattened_date_df.show()

print("d. Apply md5 masking algorithm to the password column")
flattened_date_maskedpwd_df = flattened_date_df.withColumn("password", md5(col("password")))
flattened_date_maskedpwd_df.show()

print("e. Store the cell number in the format of 000-000-0000")
flattened_date_maskedpwd_format_cell_df = flattened_date_maskedpwd_df.withColumn("cell", regexp_replace("cell", '-', '')).withColumn("cell",concat(substring("cell", 0, 3), lit("-"), substring("cell", 3, 3), lit("-"), (substring("cell", 6, 4))))
flattened_date_maskedpwd_format_cell_df.show()

print("f. Calculate the age is calculated correctly using the dob compared with the age column")
flattened_date_maskedpwd_format_cell_df.withColumn("calc_age", format_number(months_between(current_date().cast('string'), col("dob_dt_date").cast('string'), True).cast("int")/lit(12), 0))\
    .withColumn("age_validation", when(col("dob_age") == col("calc_age"), "Given age in the age col is correct").otherwise("Given age in the age col is wrong"))
#or
flattened_date_maskedpwd_format_cell_df.select("*", (year(current_date()).cast('int') - year(col('dob_dt_date'))).cast('int').alias("calc_age"))\
    .withColumn("age_validation", when(col("dob_age") == col("calc_age"), "Given age in the age col is correct").otherwise("Given age in the age col is wrong"))
print("\n we can calc the age using relativedelta from dateutil for exact result with the help of UDF and then compare with the existing one")


def years_calc(end_date, start_date):
    return relativedelta.relativedelta(datetime.strptime(end_date, "%Y-%m-%d"),datetime.strptime(start_date, "%Y-%m-%d")).years


years_calc_UDF = udf(years_calc)

flattened_date_maskedpwd_format_cell_validate_age_df = flattened_date_maskedpwd_format_cell_df.withColumn("calc_age", years_calc_UDF(current_date().cast("string"), col("dob_dt_string"))).\
    withColumn("age_validation", when(col("dob_age") == col("calc_age"), "Given age in the age col is correct").otherwise("Given age in the age col is wrong"))
flattened_date_maskedpwd_format_cell_validate_age_df.show(5, False)

print("g. Identify the email is valid")
flattened_date_maskedpwd_format_cell_validate_age_df.filter("email like '%@example.com'")
#or
flattened_date_maskedpwd_format_cell_validate_age_email_df = flattened_date_maskedpwd_format_cell_validate_age_df.select("*", expr("case when email like '%@example.com' then 'valid email' else 'Invalid email' end as valid_mailid"))
flattened_date_maskedpwd_format_cell_validate_age_email_df.show(5, False)