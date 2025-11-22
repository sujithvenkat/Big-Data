from Part_A_CoreRDD_Transformations import *
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, DateType
from pyspark.sql.functions import to_date, concat, col, current_date, current_timestamp, trim
from pyspark.sql.functions import udf
from org.inceptez.hack.allmethods import remspecialchar
hdfs_location = "E:\\BigData\\Shared_Documents\\sample_data\\sparkhack2\\"
print("19. Dataframe creation using the built in modules")


print("""19.A Create first structuretypes for all the columns as per the insuranceinfo1.csv with the columns such as 
IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,NetworkName,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan """)
structuretypes1 = StructType([StructField("IssuerId", IntegerType(), False),
                             StructField("IssuerId2", IntegerType(), False),
                             StructField("BusinessDate", DateType(), False),
                             StructField("StateCode", StringType(), False),
                             StructField("SourceName", StringType(), False),
                             StructField("NetworkName", StringType(), False),
                             StructField("NetworkURL", StringType(), True),
                             StructField("custnum", StringType(), True),
                             StructField("MarketCoverage", StringType(), True),
                             StructField("DentalOnlyPlan", StringType(), True)])
print("""19.B . Create second structuretypes for all the columns as per the insuranceinfo2.csv with the columns such as 
IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,NetworkName,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan""")
structuretypes2 = StructType([StructField("IssuerId", IntegerType(), False),
                             StructField("IssuerId2", IntegerType(), False),
                             StructField("BusinessDate", StringType(), False),
                             StructField("StateCode", StringType(), False),
                             StructField("SourceName", StringType(), False),
                             StructField("NetworkName", StringType(), False),
                             StructField("NetworkURL", StringType(), False),
                             StructField("custnum", StringType(), True),
                             StructField("MarketCoverage", StringType(), True),
                             StructField("DentalOnlyPlan", StringType(), True)])
print("""19.C . Create third structuretypes for all the columns as per the insuranceinfo2.csv with the columns such as 
IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,NetworkName,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan,RejectRows""")
structuretypes3 = StructType([StructField("IssuerId", IntegerType(), False),
                             StructField("IssuerId2", IntegerType(), False),
                             StructField("BusinessDate", StringType(), False),
                             StructField("StateCode", StringType(), False),
                             StructField("SourceName", StringType(), False),
                             StructField("NetworkName", StringType(), False),
                             StructField("NetworkURL", StringType(), False),
                             StructField("custnum", StringType(), True),
                             StructField("MarketCoverage", StringType(), True),
                             StructField("DentalOnlyPlan", StringType(), True),
                             StructField("RejectRows", StringType(), True)])


print("""20.A Create dataframe using the csv module accessing the insuranceinfo1.csv file and remove the footer from both using header true and remove the footer using dropmalformed 
options and apply the schema of the structure type created in the step 19.A""")
insuredata_df = spark.read.csv(location + "insuranceinfo1.csv", mode='dropmalformed', schema=structuretypes1, header=True)
insuredata_df.printSchema()
len(insuredata_df.collect())

print("""20.B Create another dataframe using the csv accessing the insuranceinfo2.csv file and remove the header from both dataframes using header true and remove the footer 
using dropmalformed options and apply the schema of the structure type created in the step 19.B""")
insuredata2_df = spark.read.csv(location + "insuranceinfo2.csv", mode='dropmalformed', schema=structuretypes2, header=True)
insuredata2_df.printSchema()
len(insuredata2_df.collect())
print("""As the BusinessDate column format is dd-MM-yyyy, we need to explicitly convert it to yyyy-MM-dd formatn""")
insuredata2_df = insuredata2_df.withColumn("BusinessDate", to_date("BusinessDate", "dd-MM-yyyy"))
insuredata2_df.printSchema()
insuredata2_df.show(2)

print("""20.C Create another dataframe using the csv accessing the insuranceinfo2.csv file and remove the header from the dataframe
using header true, permissive options and apply the schema of the structure type created in the step 19.C""")
rejected_df = spark.read.csv(location + "insuranceinfo2.csv", mode='permissive', schema=structuretypes3, header=True, columnNameOfCorruptRecord='RejectRows')
rejected_df.cache()
rejected_df.select("RejectRows").where("RejectRows is not null").show()
print("II. Ignoreleading and trailing whitespaces")
for rej_col_name in rejected_df.columns:
    rejected_df.withColumn(rej_col_name,trim(col(rej_col_name)))
rejected_df.coalesce(1).write.mode("overwrite").csv(location + "\\write\\rejectedrows_df", header=True)


print("21. Apply the below DSL functions in the DFs created in step 20")
insuredata_merged = insuredata_df.union(insuredata2_df)
print("21.a. Rename the fields StateCode and SourceName as stcd and srcnm respectively.")
insuredata_col_rename_df1 = insuredata_merged.withColumnRenamed("StateCode", "stcd").withColumnRenamed("SourceName", "srcnm")
insuredata_col_rename_df1.show(2)


print("21.b. Concat IssuerId,IssuerId2 as issueridcomposite and make it as a new field ")
insuredata_concat_df2 = insuredata_col_rename_df1.withColumn("issueridcomposite", concat(col("IssuerId").cast("string"), col("IssuerId2").cast("string")))
insuredata_concat_df2.show(2)


print("21.C. Remove DentalOnlyPlan column")
insuredata_drop_col_df3 = insuredata_concat_df2.drop("DentalOnlyPlan")
insuredata_drop_col_df3.show(2)


print("21.d. Add columns that should show the current system date and timestamp with the fields name of sysdt and systs respectively.")
insuredata_add_sysdt_time_df4 = insuredata_drop_col_df3.withColumn("sysdt", current_date()).withColumn("systm", current_timestamp())
insuredata_add_sysdt_time_df4.show(2)


print("Separate usecases")
print("i. Identify all the column names and store in an List variable – use columns function.")
col_list = insuredata_add_sysdt_time_df4.columns


print("ii. Identify all columns with datatype and store in a list variable and print use dtypes function")
col_list_dtypes = insuredata_add_sysdt_time_df4.dtypes


print("iii. Identify all integer columns alone and store in an list variable and print.")
col_dict_dtypes = dict(col_list_dtypes)
col_int_types = []
for col_name in col_dict_dtypes:
    if col_dict_dtypes[col_name] == 'int':
        col_int_types.append(col_name)
print(f"integer columns list is {col_int_types}")


print("iv. Select only the integer columns identified in the above statement and show 10 records in the screen")
insuredata_add_sysdt_time_df4.select(col_int_types).show(10)

print("v. Identify the additional column in the reject dataframe created in step 20 above by subtracting the "
      "columns between dataframe1 and dataframe3 created in step20.")
insuredata_df1_col_list = insuredata_df.columns
reject_col_list = rejected_df.columns
insur_cols = set(insuredata_df1_col_list)
reject_rows_cols = set(reject_col_list)
additional_cols = reject_rows_cols - insur_cols
print(f"The additional columns are {additional_cols}")


print("""22. Take the DF created in step 21.d and Remove the rows contains null in any one of the 
field and count the number of rows which contains all columns with some value. """)
insuredata_drop_null_df5 = insuredata_add_sysdt_time_df4.na.drop('any')
insuredata_drop_null_df5.show(2)
insuredata_drop_null_df5.count()


print("23. Custom Method creation:")
remspecialchar_UDF = udf(remspecialchar)


print("25. Call the above udf in the DSL by passing NetworkName column as an argument to get the special characters removed DF.")
insuredata_udf_df6 = insuredata_drop_null_df5.withColumn("NetworkName", remspecialchar_UDF("NetworkName"))
insuredata_udf_df6.select("NetworkName").show(40, False)


print("26. Save the DF generated in step 25 in JSON format into HDFS with overwrite option.")
insuredata_udf_df6.write.mode("overwrite").json(hdfs_location + "/insuredata_udf_json")


print("27. Save the DF generated in step 25 into CSV format with header name as per the DF and "
      "delimited by ~ into HDFS with overwrite option ")
insuredata_udf_df6.write.mode("overwrite").csv(hdfs_location + "/insuredata_udf_csv", header= True, sep="~")


def writeToFile(sparksession, df, filetype, location, delimiter, md):
    if filetype == 'csv':
        df.write.mode(md).csv(location, header=True, sep=delimiter)
    elif filetype == 'json':
        df.write.mode(md).json(location)


writeToFile(spark, insuredata_udf_df6, 'csv', location + "\\write\\insuredata2_udf", "~", "overwrite")
writeToFile(spark, insuredata_udf_df6, 'json', location + "\\write\\insuredata2_udf_json", "", "overwrite")


print("""28. Save the DF generated in step 25 into hive external table and append the data without 
overwriting it""")
insuredata_udf_df6.write.mode("append").saveAsTable("cust_prof_part_tbl")



print("4. Tale of handling RDDs, DFs and TempViews (20% Completion) – Total 75%")
print("""Loading RDDs, split RDDs, Load DFs, Split DFs, Load Views, Split Views, write UDF, register to 
use in Spark SQL, Transform, Aggregate, store in disk/DB""")


print("Use RDD functions:")
print("""29. Load the file3 (custs_states.csv) from the HDFS location, using textfile API in an RDD custstates, this 
file contains 2 type of data one with 5 columns contains customer master info and other data with statecode and 
description of 2 columns.""")
custstates = sc.textFile(hdfs_location + "custs_states.csv")


print("""30. Split the above data into 2 RDDs, first RDD namely custfilter should be loaded only with 5 columns data 
and second RDD namely statesfilter should be only loaded with 2 columns data.""")
custstates_split = custstates.map(lambda x: x.split(",")).map(lambda x: (x,len(x)))
custstates_split.cache()

custfilter = custstates_split.filter(lambda x: x[1] == 5).map(lambda x: x[0])
statesfilter = custstates_split.filter(lambda x: x[1] == 2).map(lambda x: x[0])

custfilter.take(5)
custfilter.count()  # 810

statesfilter.take(5)
statesfilter.count()  # 51


print("Use DSL functions:")
print("""31. Load the file3 (custs_states.csv) from the HDFS location, using CSV Module in a DF custstatesdf, this file
contains 2 type of data one with 5 columns contains customer master info and other data with statecode and description 
of 2 columns.""")
custstatesdf = spark.read.csv(hdfs_location + "custs_states.csv", header=False)


print("""32. Split the above data into 2 DFs, first DF namely custfilterdf should be loaded only with 5 columns data 
and second DF namely statesfilterdf should be only loaded with 2 columns data""")
custfilterdf_pre = custstatesdf.where("_c3 is not null or _c4 is not null")
custfilterdf = custfilterdf_pre.select(col("_c0").cast("int").alias("custid"), col("_c1").alias("custfname"), col("_c2").alias("custlname"),
                    col("_c3").cast("int").alias("custage"),col("_c4").alias("custprofession"))
custfilterdf.printSchema()
custfilterdf.show(5)

statesfilterdf_pre = custstatesdf.filter("_c3 is null and _c4 is null")
statesfilterdf = statesfilterdf_pre.select(col("_c0").alias("statecode"), col("_c1").alias("statename"))
statesfilterdf.printSchema()
statesfilterdf.show(5)


print("Use SQL Queries:")

print("33. Register the above step 32 two DFs as temporary views as custview and statesview.")
custfilterdf.createOrReplaceTempView("custview")
statesfilterdf.createOrReplaceTempView("statesview")
insuredata_add_sysdt_time_df4.createOrReplaceTempView(("insureview"))
spark.udf.register("remspecialchar",remspecialchar)


print("36. Write an SQL query with the below processing – set the spark.sql.shuffle.partitions to 4")
spark.conf.set("spark.sql.shuffle.partitions",4)
print("36.a. Pass NetworkName to remspecialcharudf and get the new column called cleannetworkname")
print("36.b. Add current date, current timestamp fields as curdt and curts.")
print("36.c  Extract the year and month from the businessdate field and get it as 2 new fields called yr,mth respectively.")
print("36.d. Extract from the protocol either http/https from the NetworkURL column, if http then print http non secured"
      " if https then secured else no protocol found then display noprotocol.")
# since we are checking https  in the first when condition, all the https will get passed in the first when itself only the failures will move to second when so http will get cover in the second when
insuredata_udf_dt_time_yr_mth_protocol_sql = spark.sql("select IssuerId, IssuerId2, BusinessDate, stcd, srcnm, remspecialchar(NetworkName) cleannetworkname, "
          "NetworkURL, custnum, MarketCoverage, issueridcomposite, current_date() curdt, current_timestamp() curts, "
          "year(BusinessDate) yr, month(BusinessDate) mth, case when NetworkURL like 'https%' then 'http secured' "
          "when NetworkURL like 'http%' then 'http non secured' else 'no protocol' end as protocol from insureview")
insuredata_udf_dt_time_yr_mth_protocol_sql.show(10, False)
insuredata_udf_dt_time_yr_mth_protocol_sql.createOrReplaceTempView("insureview_detailed")


print("""36.e. Display all the columns from insureview including the columns derived from above a, b, c, d steps with statedesc column 
from statesview with age,profession column from custview . Do an Inner Join of insureview with statesview using stcd=stated 
and join insureview with custview using custnum=custid.""")
insuredata_join_sql = spark.sql("select t1.*, t2.statename,t3.custage, t3.custprofession "
          "from insureview_detailed t1 "
          "join "
          "statesview t2 "
          "on t1.stcd = t2.statecode "
          "join "
          "custview t3 "
          "on t1.custnum = t3.custid")
insuredata_join_sql.show(10, False)


print("37. Store the above selected Dataframe in Parquet formats in a HDFS location as a single file.")
insuredata_join_sql.coalesce(1).write.mode("overwrite").parquet(hdfs_location + "/insuredata_sql_op")


print("""38. Write an SQL query to identify average age, count group by statedesc, protocol, 
profession including a seqno column added which should have running sequence 
number partitioned based on protocol and ordered based on count descending and 
display the profession whose second highest count of a given state and protocol.
""")
insuredata_join_sql.createOrReplaceTempView("Enriched_insureview")
insuredata_aggregated = spark.sql("""select * from (select row_number() over(partition by statename, protocol order by count desc) Seqno, Avgage, count, statename, protocol, custprofession from 
(select statename, protocol, custprofession, avg(custage) as Avgage, count(*) as count from Enriched_insureview where custprofession is not null group by statename, protocol, custprofession) temp) temp1 where Seqno = 2""")
insuredata_aggregated.show(10)


print("39. Store the DF generated in step 38 into MYSQL table insureaggregated.")
insuredata_aggregated.write.jdbc(url="jdbc:mysql://127.0.0.1:3306/custdb?user=root&password=Root123$", table="insuredata_aggregated",mode="overwrite",properties={"driver": 'com.mysql.jdbc.Driver'})








'''
cust_struct = StructType([StructField("custid", IntegerType(), True),
                          StructField("custfname", StringType(), False),
                          StructField("custlname", StringType(), False),
                          StructField("custage", IntegerType(), False),
                          StructField("custprofession", StringType(), False)])
states_struct = StructType([StructField("statecode", StringType(), False),
                            StructField("statename", StringType(), False)])
custfilterdf.select("_c0").where("upper(_c0) <> lower(_c0)")
spark.createDataFrame(custfilterdf.rdd, cust_struct)
spark.createDataFrame(statesfilterdf.rdd, states_struct)
'''

