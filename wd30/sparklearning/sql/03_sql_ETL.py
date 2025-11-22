#import findspark
#findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ShortType
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("Spark_SQL_ETL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("***************1. Data Munging *********************")

cust_df = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified")
cust_df.count()
cust_df.show()
'''
+-------+---------+----------+----+---------+
|    _c0|      _c1|       _c2| _c3|      _c4|
+-------+---------+----------+----+---------+
|   null|    Karen|   Puckett|  74|   Lawyer|
|4000006|  Patrick|      Song|  24|     null|
|    ten|    Elsie|  Hamilton|  43|    Pilot|
|   null|    Hazel|    Bender|  63|     null|
|   null|     null|      null|null|     null|
'''

# while reading the file itself we can decide whether we need this null (unclean) or not with the help of the blow modes,

# mode- permissive (default)- permit all the data including the unclean data
# mode- failfast - as soon as you see some unwanted data, fail our program
# mode- dropmalformed - when there are unclean data (doesn't fit with the structure (customschema)/columns are lesser than the defined(custom schema)/identified(inferschema)) don't consider them

cust_df_perm = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified", mode="permissive")
cust_df_fail = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified", mode="failfast")
cust_df_dropmal = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified", mode="dropmalformed")
cust_df_perm.count()
cust_df_perm.printSchema()
'''
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: string (nullable = true)
 |-- _c4: string (nullable = true)
'''
#we can apply the above mode while reading
#cust_df_perm.count() is same as cust_df_dropmal.count() because we have to apply schema and corrupt records since it is a csv file

#don't use count rather use len(collect)
print(cust_df_perm.count())
print(len(cust_df_perm.collect()))

cust_df_perm_infer = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified", mode="permissive", inferSchema=True)
cust_df_perm_infer.show()
cust_df_perm_infer.printSchema()
'''
root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: integer (nullable = true)
 |-- _c4: string (nullable = true)
'''

print("""1 . The _c0 column shoukd be integer, its a kind of id but print schema shows string bcs some record has string value in that column 
we have to find that """)

# cust_df_perm_infer.selectExpr("select _c0")
# we have to use where here bcs we are going to filer the column
col_0_issue_records = cust_df_perm_infer.where("upper(_c0)<>lower(_c0)")
col_0_issue_records.show()

# identify null record in col0 because that is the identity column we can say like SN number
col_0_null_records = cust_df_perm_infer.where("_c0 is null")
col_0_null_records.show()

cust_df_perm_infer.select("_c0").count()  # 10005
cust_df_perm_infer.select("_c0").distinct().count()  # 10000
#len(cust_df_perm_infer.collect()) # 10005
'''
>>> cust_df_perm_infer = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified", mode="permissive", inferSchema=True)
>>> len(cust_df_perm_infer.collect())
10005
>>> cust_df_perm_infer.printSchema()
root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: integer (nullable = true)
 |-- _c4: string (nullable = true)
'''


# Apply inferschema to failfast and dropmalformed
cust_df_fail_infer = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified", inferSchema=True, mode="failfast")
cust_df_fail_infer.count() # 10005

# Count will not fail because its not reading the malformed data
# inferschema required 5 columns, but only 4 columns are found, so program is failing
# 4000006,Patrick,Song,24
# trailer_data:end of file
# but show will fail since its reading malformed data
cust_df_fail_infer.show()
cust_df_fail_infer.printSchema()

cust_df_dropmal_infer = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified", inferSchema=True, mode="dropmalformed")
len(cust_df_dropmal_infer.collect())
cust_df_dropmal_infer.printSchema()
cust_df_dropmal_infer.where("upper(_c0)<>lower(_c0)").show()
'''
>>> cust_df_dropmal_infer = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified", inferSchema=True, mode="dropmalformed")
>>> len(cust_df_dropmal_infer.collect())
10003
>>> cust_df_dropmal_infer.printSchema()
root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: integer (nullable = true)
 |-- _c4: string (nullable = true)
'''

cust_df_perm_infer.select("*").subtract(cust_df_dropmal_infer.select("*")).show(10)
# if you want to see the malformed data you have to create schema structure with corrupt column since csv is as intelligent as json
# Lets create struct

cust_struct = StructType([StructField("id", IntegerType(), False),
                          StructField("custfname", StringType(), False),
                          StructField("custlname", StringType(), True),
                          StructField("custage", ShortType(), True),
                          StructField("custprofession", StringType(), True),
                          StructField("corrupt_data", StringType(),True)])  # workaround1 - adding the corrupt_data derived column

# Now read the data with permissive mode with the schema structure we can get the data filtered,

cust_df_perm_struct = spark.read.schema(cust_struct).csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified", mode="permissive", inferSchema=True)
cust_df_perm_struct = spark.read.schema(cust_struct).csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified", mode="permissive")
cust_df_perm_struct.count()

#  No diff in the above lines because we didn't mention corrupt column name while reading
cust_df_perm_struct = spark.read.schema(cust_struct).csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified",
                                                         mode="permissive",
                                                         inferSchema=True,
                                                         columnNameOfCorruptRecord="corrupt_data")
cust_df_perm_struct.printSchema()
cust_df_perm_struct.show(10, False)
cust_df_perm_struct = spark.read.schema(cust_struct).csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified",
                                                         mode="permissive",
                                                         columnNameOfCorruptRecord="corrupt_data")
cust_df_perm_struct.where("upper(id)<>lower(id)").show()
cust_df_perm.where("upper(_c0)<>lower(_c0)").show()
len(cust_df_perm_struct.collect())
cust_df_perm_struct.select("id").where("custfname = 'Elsie' and custfname = 'Hamilton'").show()
cust_df_perm_struct.count()
# we can declare the schema any where either inside the csv () or using .schema()
# Even If we mention inferschema = true and schema it will take the schema as the main one.

cust_df_perm_struct = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified",
                                                         mode="permissive",
                                                         inferSchema=True,
                                                         schema=cust_struct,
                                                         columnNameOfCorruptRecord="corrupt_data")
cust_df_perm_struct = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified",
                                                         mode="permissive",
                                                         schema=cust_struct,
                                                         columnNameOfCorruptRecord="corrupt_data")

# If we want to see the corrupt data we can get that using the select clause
cust_df_corrupt_recs = cust_df_perm_struct.select("corrupt_data")
cust_df_corrupt_recs.show()

# We can't directly run the corrupt rec query instead we can save the parsed results and then we can see the corrupt records
# Also the corrupt cols may have null values so we must use not null in col query
cust_df_perm_struct.cache()
# We have to cache the main df not the one where the corrupt records got saved
cust_df_corrupt_recs = cust_df_perm_struct.select("corrupt_data").where("corrupt_data is not null")
cust_df_corrupt_recs.show(10, False)
cust_df_corrupt_recs.write.mode("overwrite").csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\rejected_recs")
'''
+---------------------------+
|corrupt_data               |
+---------------------------+
|4000006,Patrick,Song,24    |
|ten,Elsie,Hamilton,43,Pilot|
|trailer_data:end of file   |
+---------------------------+
'''
#TYPE MISMATCH, MISSING COLUMNS
# The schema we gave has 5 columns so 4 columns cam into the corrupt record and data type is string for id col so ten cam here and the
# last one is trailer record

# Since we found the duplicate records lets create a good schema without corrupt column and load
# From the above process we found this custmodified file has following issues,
# 1. duplicates record wise and column wise
# 2. null values in _c0
# 3. Type mismatches
# 4. The columns greater or lesser than the columns we provided in the schema struct > or < 5

# We have used the columnNameOfCorruptRecord option in csv file to get rid of the issues in 3 and 4th points
# lets create a good schema without corrupt column and load
cust_struct1 = StructType([StructField("id", IntegerType(), False),
                          StructField("custfname", StringType(), False),
                          StructField("custlname", StringType(), True),
                          StructField("custage", ShortType(), True),
                          StructField("custprofession", StringType(), True)])

cust_df_perm_struct1 = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified",
                                                         mode="permissive",
                                                         schema=cust_struct1)

cust_df_perm_struct1.show(10, False)
len(cust_df_perm_struct1.collect()) #10005
cust_df_perm_struct1.write.mode("overwrite").csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\permissive_recs")

# now we can use dropmalformed to drop all culprits records
cust_df_fail_struct1 = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified",
                                                         mode="failfast",
                                                         schema=cust_struct1)
# failfast will still fail since we didnt remove those records yet we can use dropmalformed
cust_df_dropmal_struct1 = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified",schema=cust_struct1, mode="dropmalformed")
cust_df_dropmal_struct1.select("*").where("id == 'ten'").show()
cust_df_dropmal_struct1.where("upper(id)<>lower(id)").show()
cust_df_dropmal_struct1.where("upper(id)==lower(id)").show()
cust_df_dropmal_struct1.write.mode("overwrite").csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\dropmal_recs")
cust_df_dropmal_struct1.count()  # 10005
len(cust_df_dropmal_struct1.collect())  # 10002

cust_df_perm_struct1.select(col("id").alias("perm_id"), col("custfname").alias("prm_name")).\
    subtract(cust_df_dropmal_struct1.select(col("id").alias("drp_id"), col("custfname").alias("drp_name"))).show(10, False)
#  IF you see the .count() fn gives 10005 but the actual records in cust_df_dropmal_struct1 is just 10002, so instead of
#  using .count() in dropmalformed we have to use len(df.collect())

# We can send back the corrupted recrds back to the source either by writting into dropmal_records like above or
# We can also get that as a df with the help of subtract from all recs

'''
If we use inferschema and mode permissive all the records will get processed
If we use inferschema and mode malformed  the malformed recs like below got dropped,
+--------------------+-------+----+----+----+
|                 _c0|    _c1| _c2| _c3| _c4|
+--------------------+-------+----+----+----+
|trailer_data:end ...|   null|null|null|null|
|             4000006|Patrick|Song|  24|null|
+--------------------+-------+----+----+----+
but still it allowed this rec without dropping even string present in int column, bcs inferschema consider the entire col as
string if it has only one col with string and not dropping

+---+-----+--------+---+-----+
|_c0|  _c1|     _c2|_c3|  _c4|
+---+-----+--------+---+-----+
|ten|Elsie|Hamilton| 43|Pilot|
+---+-----+--------+---+-----+

To get clean even the above, then we have to go with 
own schema struct, in that way "ten" will ge deleted
Next we can go with mode permissive with own schema, if we do that these types of records will no get deleted.

+-------+---------+---------+-------+--------------+
|id     |custfname|custlname|custage|custprofession|
+-------+---------+---------+-------+--------------+
|4000006|Patrick  |Song     |24     |null          |
|null   |Elsie    |Hamilton |43     |Pilot         |
+-------+---------+---------+-------+--------------+

we have to use dropmalformed to avoid these recs too

'''
# we can get summary of the df

cust_df_dropmal_struct1.describe().show(10)
cust_df_dropmal_struct1.summary().show()

# now if you see we hve null values in the key column, w=if we want to clean null values we can convert to rdd and back to DF
# with our predefine schema
# but we can't delete but we can fail our program if any of our key column has null
'''
>>> cust_df_dropmal_struct1.show()
+-------+---------+----------+-------+--------------------+
|     id|custfname| custlname|custage|      custprofession|
+-------+---------+----------+-------+--------------------+
|4000000|   Apache|     Spark|     11|                null|
|4000001| Kristina|     Chung|     55|               Pilot|
|4000001| Kristina|     Chung|     55|               Pilot|
|4000002|    Paige|      Chen|     77|               Actor|
|4000003|   Sherri|    Melton|     34|            Reporter|
|4000003|  mohamed|     irfan|     41|                  IT|
|4000003|vaishnavi| santharam|     30|                  IT|
|4000004| Gretchen|      null|     66|                null|
|   null|    Karen|   Puckett|     74|              Lawyer|
|   null|    Hazel|    Bender|     63|                null|
|   null|     null|      null|   null|                null|
|4000009|  Malcolm|      null|     39|              Artist|
'''

cust_df_dropmal_struct1.rdd.toDF(cust_struct1).show()
#cust_df_dropmal_struct1.unionByName()

print("a. Combining Data")

df_csv = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\src1", inferSchema=True, header=True, sep="~")
# here it brings all the csv file present inside src1 folder but not from subfolders inside the same src1.
# to acheive that we have to use recursive option
df_csv_all = spark.read.option("recursiveFileLookup", "true").csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\src1\\",inferSchema=True, header=True, sep="~")
#or
df_csv_all = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\src1\\",inferSchema=True, header=True, sep="~", recursiveFileLookup=True)
df_csv_all.count()  # 30
df_csv_all = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\src1\\nyse*",inferSchema=True, header=True, sep="~", recursiveFileLookup=True)
df_csv_all.count()  # 18
df_csv_path = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\src1\\", recursiveFileLookup=True, sep="~", pathGlobFilter="nyse[1-3].csv", header=True)
df_csv_path.count()  # 28
# The above function will recurse the subdirectories and find the patter of nyse1.csv and nyse2.csv

# to read file from diff directories isntead of subdirectories,

df_csv_diff = spark.read.csv(path=["E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\src1\\", "E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\src3\\"],
                             header=True, sep="~", pathGlobFilter="nyse[1-4].csv")
df_csv_diff.count()  # 24
# he above function will search in the main and recurse the subdirectories and find the patter of nyse1.csv and nyse2.csv
'''
df_csv_diff.withColumn("stock",col("stock").upper("stock").alias("upper_stock")).show()
df_csv_diff.withColumn("stock", col("stock2")).show()
df_csv_diff.withColumn("incr",col("stock").cast()))
print("b. Schema Merging (Structuring)")
'''

print("b. Schema Merging (Structuring)")

# the above combining different csv will work only when both the schemna are same, if they are different then this method will fail

df_csv_schema = spark.read.csv(path=["E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\stockdata\\",
                                     "E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\stockdata1\\"],
                                     inferSchema=True, header=True, sep="~")
df_csv_schema.show()
df_csv_schema.count() # 9

'''
+-----+------+-----+-------+------+
|stock| value| incr|maxrate|   cat|
+-----+------+-----+-------+------+
|  HUL| 450.3|  neg|    710|retail|
|  RAN|754.62|  pos|    900|pharma|
|  CIP|1000.2|  pos|1210.50|pharma|
|  HUL| 450.3|   -1|    710|   300|
|  RAN|754.62|   10|    900|   500|
|  CIP|1000.2|   20|1210.50|   200|
| NYSE|   CLI| 35.3|    1.1|   EST|
| NYSE|   CVH|24.62|      2|   EST|
| NYSE|   CVL| 30.2|     11|   EST|
+-----+------+-----+-------+------+
'''
df_csv_schema2 = spark.read.csv(path=["E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\stockdata\\",
                                     "E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\stockdata2\\"],
                                     inferSchema=True, header=True, sep="~")
df_csv_schema2.show()
#above methodology leads to a wrong result
# Here all the values assigned to different columns because pf schema differewnce
# to avoid this we have to use unionbyname and allow missingscolumn to true

# we have to create oonew full staructure as per our need
# stock~value~maxrate~minrate~dur_mth~incr
# stock~value~incr~maxrate~cat
# stock~value~incr~maxrate~ipo

# we no need to create any schema too, we can unionbyname to df with different schema and allowmissingcolumns we will get all col
cols_struct_csv = StructType([StructField('stock', StringType(), True),
                             StructField('value', StringType(), True),
                             StructField('maxrate', StringType(), True),
                             StructField('minrate', StringType(), True),
                             StructField('dur_mth', StringType(), True),
                             StructField('incr', StringType(), True),
                             StructField('cat', StringType(), True),
                             StructField('ipo', StringType(), True)])

df_csv_schema1 = spark.read.csv(path=["E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\stockdata\\"],
                        inferSchema=True, header=True, sep="~")
df_csv_schema1.printSchema()
df_csv_schema2 = spark.read.csv(path=["E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\stockdata2\\"],
                        inferSchema=True, header=True, sep="~")
df_csv_schema2.printSchema()
df_csv_union = df_csv_schema1.unionByName(df_csv_schema2, allowMissingColumns=True)
df_csv_union.show()
print("b.3. Schema Evolution (Structuring) - source data is evolving with different structure")

spark_hr1_df = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\stockdata\\bse1.csv",
                              inferSchema=True, header=True, sep="~")
spark_hr1_df.write.parquet("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\write\\stockdata\\bse1_parquet",
                           mode="overwrite")
spark_hr2_df = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\stockdata\\bse2.csv",
                              inferSchema=True, header=True, sep="~")
spark_hr2_df.show()
spark_hr2_df.printSchema()
# now oinstead of column cat we have new column ipo

# so we are adding that new column here wth the old column
# If we didnt do unionbyname or anywork around to handle this then we will get this below issue

spark_hr1_hr2_df = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\stockdata\\",
                              inferSchema=True, header=True, sep="~")
spark_hr1_hr2_df.show()
spark_hr1_hr2_df.printSchema()

'''
+-----+------+----+-------+------+
|stock| value|incr|maxrate|   cat|
+-----+------+----+-------+------+
|  HUL| 450.3| neg|    710|retail|
|  RAN|754.62| pos|    900|pharma|
|  CIP|1000.2| pos|1210.50|pharma|
|  HUL| 450.3|  -1|    710|   300|
|  RAN|754.62|  10|    900|   500|
|  CIP|1000.2|  20|1210.50|   200|
+-----+------+----+-------+------+
'''
# we lost ipo column above so we have to do workaround
spark_hr2_df.printSchema()  # shows incr as int so we are changing that to string soince hr1 df incr is string
spark_hr2_df = spark_hr2_df.withColumn("incr",col("incr").cast("string"))
spark_hr2_df.printSchema()

spark_hr2_df.write.parquet("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\write\\stockdata\\bse1_parquet",
                           mode="append")

# now we can read the single parquet files wth different schema data appended, now we dont need to do the union name since
# parquet is an intelligent schema

spark_hr1_hr2_df_pq = spark.read.parquet("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\write\\stockdata\\bse1_parquet")
spark_hr1_hr2_df_pq.show()
'''
+-----+------+----+-------+------+
|stock| value|incr|maxrate|   cat|
+-----+------+----+-------+------+
|  HUL| 450.3| neg|  710.0|retail|
|  RAN|754.62| pos|  900.0|pharma|
|  CIP|1000.2| pos| 1210.5|pharma|
|  HUL| 450.3|  -1|  710.0|  null|
|  RAN|754.62|  10|  900.0|  null|
|  CIP|1000.2|  20| 1210.5|  null|
+-----+------+----+-------+------+
'''
# now we can add mergeSchema as True to see all the columns
spark_hr1_hr2_df_pq_merge = spark.read.parquet("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\write\\stockdata\\bse1_parquet",
                                               mergeSchema=True)
spark_hr1_hr2_df_pq_merge.show()
'''
+-----+------+----+-------+------+----+
|stock| value|incr|maxrate|   cat| ipo|
+-----+------+----+-------+------+----+
|  HUL| 450.3| neg|  710.0|retail|null|
|  RAN|754.62| pos|  900.0|pharma|null|
|  CIP|1000.2| pos| 1210.5|pharma|null|
|  HUL| 450.3|  -1|  710.0|  null| 300|
|  RAN|754.62|  10|  900.0|  null| 500|
|  CIP|1000.2|  20| 1210.5|  null| 200|
+-----+------+----+-------+------+----+
'''

# to use the above merge schema then the common columns present in both the df should have same datatype, in this case it
# is "incr" that's why we cast it to string from int else we will get error

# Failed to merge fields 'incr' and 'incr'. Failed to merge incompatible data types string and int

# here we have only oone column so we easily did, so in real time better we create a own structure type to acheive this

cols_struct_orc = StructType([StructField('stock', StringType(), True),
                             StructField('value', StringType(), True),
                             StructField('incr', StringType(), True),
                             StructField('maxrate', StringType(), True),
                             StructField('cat', StringType(), True),
                             StructField('ipo', StringType(), True)])

spark_hr1_df_own_schema = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\stockdata\\bse1.csv",
                              inferSchema=True, header=True, sep="~")
spark_hr1_df_own_schema.write.orc("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\write\\stockdata\\bse1_orc",
                           mode="overwrite")
spark_hr2_df_own_schema = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\stockdata\\bse2.csv",
                              inferSchema=True, header=True, sep="~")
spark_hr2_df_own_schema.write.orc("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\write\\stockdata\\bse1_orc",
                           mode="append")

spark_hr1_hr2_orc_df = spark.read.orc("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\write\\stockdata\\bse1_orc")
spark_hr1_hr2_orc_df.show()
spark_hr1_hr2_orc_df = spark.read.orc("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\write\\stockdata\\bse1_orc",
                                    mergeSchema=True)
spark_hr1_hr2_orc_df.show()
spark.createDataFrame(spark_hr1_hr2_orc_df.rdd,cols_struct_orc).show()

spark_hr1_df_own_schema.write.parquet("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\write\\stockdata\\bse1_pq",
                           mode="overwrite")
spark_hr2_df_own_schema.write.parquet("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\write\\stockdata\\bse1_pq",
                           mode="append")
spark_hr1_hr2_pq_df = spark.read.parquet("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\write\\stockdata\\bse1_pq",mergeSchema=True)
spark.createDataFrame(spark_hr1_hr2_pq_df.rdd,cols_struct_orc).show()

print("c.1. Validation (active)- DeDuplication")

custstructtype1 = StructType([StructField("id", IntegerType(), False),
                              StructField("custfname", StringType(), False),
                              StructField("custlname", StringType(), True),
                              StructField("custage", ShortType(), True),
                              StructField("custprofession", StringType(), True)])

clean_df = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\custsmodified",schema=custstructtype1,
                          mode="dropmalformed")
clean_df.count()  # 10005
len(clean_df.collect()) # 10002
clean_df.printSchema()
clean_df.show(20, False)
#  culprit data in this file custsmodified are - _c0 has null, duplicates, datatype mismatch, number of columns mismatch are lesser than 5 for 2 rows
clean_df.where("id == '4000001'").show()
'''
+-------+---------+---------+-------+--------------+
|     id|custfname|custlname|custage|custprofession|
+-------+---------+---------+-------+--------------+
|4000001| Kristina|    Chung|     55|         Pilot|
|4000001| Kristina|    Chung|     55|         Pilot|
+-------+---------+---------+-------+--------------+
'''
#dedupication: we can use distinct or dropduplicates
clean_df_Dropdup = clean_df.dropDuplicates()
clean_df_Dropdup.show()
# it will drop the duplicates in records, we may have duplicates in column level too
clean_df_distinct = clean_df.distinct()
clean_df_distinct.show()

# both are same rssults
clean_df_Dropdup.where("id == '4000001'").show()
clean_df_distinct.where("id == '4000001'").show()

# theabove too wont delete column level duplicates
clean_df_Dropdup.where("id == '4000003'").show()
'''
+-------+---------+---------+-------+--------------+
|     id|custfname|custlname|custage|custprofession|
+-------+---------+---------+-------+--------------+
|4000003|   Sherri|   Melton|     34|      Reporter|
|4000003|vaishnavi|santharam|     30|            IT|
|4000003|  mohamed|    irfan|     41|            IT|
+-------+---------+---------+-------+--------------+
'''
clean_df_dedup = clean_df_Dropdup.dropDuplicates(['id'])
clean_df_dedup.where("id == '4000003'").show()

'''
+-------+---------+---------+-------+--------------+
|     id|custfname|custlname|custage|custprofession|
+-------+---------+---------+-------+--------------+
|4000003|   Sherri|   Melton|     34|      Reporter|
+-------+---------+---------+-------+--------------+
'''
# its retains the first records and deletes the rest for the same id

#we can write a spark sql quey

clean_df_dedup.createGlobalTempView("Gview1")
# this view will be across the session

clean_df.createOrReplaceTempView("view1")
spark.sql("select distinct id, custfname, custlname, custage, custprofession from view1").where("id in ('4000001','4000003')").show()
## is same as
spark.sql("select distinct * from view1").where("id in ('4000001','4000003')").show()
#spark.sql("select * from view1").where (id in (select distinct id from view1))").where("id in ('4000001','4000003')").show()
spark.sql("select * from view1 where id in ('4000001','4000003') group by id, custfname, custlname, custage, custprofession").show()
'''
+-------+---------+---------+-------+--------------+---+
|     id|custfname|custlname|custage|custprofession| rn|
+-------+---------+---------+-------+--------------+---+
|4000001| Kristina|    Chung|     55|         Pilot|  1|
|4000003|   Sherri|   Melton|     34|      Reporter|  1|
|4000003|vaishnavi|santharam|     30|            IT|  1|
|4000003|  mohamed|    irfan|     41|            IT|  1|
+-------+---------+---------+-------+--------------+---+
'''
# column level dedup
spark.sql("select distinct id, custfname, custlname, custage, custprofession from view1 where id is null").show()
clean_df_dedup_sql = spark.sql("select * from "
          "(select *, row_number() over(partition by id ORDER BY id ) as rn from"
          " view1) t1 where rn = 1 and id in ('4000001','4000003')")
clean_df_dedup_sql = spark.sql("select * from "
          "(select *, row_number() over(partition by id ORDER BY id ) as rn from"
          " view1) t1 where rn = 1 ")
'''
+-------+---------+---------+-------+--------------+---+
|     id|custfname|custlname|custage|custprofession| rn|
+-------+---------+---------+-------+--------------+---+
|4000001| Kristina|    Chung|     55|         Pilot|  1|
|4000003|   Sherri|   Melton|     34|      Reporter|  1|
+-------+---------+---------+-------+--------------+---+
'''
print("c.2. Data Preparation (Cleansing & Scrubbing")

clean_df_Dropdup.where("id is null").show()
clean_df_Dropdup.count()
len(clean_df_Dropdup.collect())

# this all will drop teh records if all the column has null values
clean_df_Dropdup.na.drop("all").count()  # 10000
clean_df_Dropdup.na.drop("any").count()  # 9912

clean_df_Dropdup.na.drop("all", subset=['id']).count()  # 9998
clean_df_Dropdup.na.drop("any", subset=['id', 'custprofession']).count()  # 9913
clean_df_Dropdup.na.drop("all", subset=['id', 'custprofession']).count()  # 9999

clean_df_drop_key_null = clean_df_Dropdup.na.drop("all", subset=['id'])
clean_df_drop_key_null.show()
len(clean_df_drop_key_null.collect())
#understanding thresh
'''
NYSE~~
NYSE~CVH~
NYSE~CVL~30.2
~~~
'''
df1 = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse_thresh.csv", sep='~')
df1.show()

df1.na.drop("all",subset=['_c0', '_c1', '_c2']).show()
df1.na.drop("all").show()
df1.na.drop("any").show()
df1.na.drop("all",subset=['_c2']).show()

# apply thresh

df1.na.drop("all", thresh=1).show()
df1.na.drop("any", thresh=1).show()
df1.na.drop("all", thresh=2).show()
df1.na.drop("any", thresh=3).show()

print("Scrubbing (convert of raw to tidy na.fill or na.replace)")
# Writing DSL

clean_df_drop_key_null.where("id is null").show()

# now its time fill null with some values instead of dropping nulll values

clean_df.where("custlname is null").show()
#clean_df_Dropdup_fill = clean_df.na.fill("NA", subset=['custlname'])  Not working
clean_df_Dropdup_fill = clean_df.fillna("NA", subset=['custlname'])
clean_df_Dropdup_fill.where("custlname == 'NA'").show()
clean_df_Dropdup_fill.where("id is null").show()
clean_df_drop_key_null.where("id is null or custlname is null or custprofession is null").show()
clean_df_drop_key_null_fill_na = clean_df_drop_key_null.fillna("NA",subset=['custlname','custprofession'])
clean_df_drop_key_null_fill_na.where("id is null or custlname is null or custprofession is null").show()
clean_df_drop_key_null_fill_na.show()

# we can fill the null values with some other valuesclean_df_drop_key_null like above also we can also assign map/dict values also
# we have clean data at clean_df_drop_key_null_fill_na

# we can replace the data with the value we wish also for eg if we want the 'Therapist' in the custprofession column to
# get replaced with the Physician we can use like below
prof_dict={"Therapist":"Physician","Musician":"Music Director","NA":"prof not defined"}
clean_df_drop_key_null_fill_na_replace = clean_df_drop_key_null_fill_na.na.replace(prof_dict,subset=['custprofession'])
clean_df_drop_key_null_fill_na_replace.where("id == '4003370'").show()

#Writing equivalent SQL - for learning the above functionality more relatively & easily and to understand which way is more feasible
#id, custfname, custlname, custage, custprofession
clean_df_dedup_sql.drop('rn').createOrReplaceTempView("clean_df_dedup_sql")
spark.sql("select * from clean_df_dedup_sql where id is null or custlname is null or custprofession is null").show()

clean_df_dedup_sql_fill_na = spark.sql("""select id, custfname, 
coalesce(custlname,'NA') custlname, coalesce(custprofession,'NA') custprofession from clean_df_dedup_sql""")
clean_df_dedup_sql_fill_na.show()
#spark.sql("select * from clean_df_dedup_sql_fill_na where id is null or custlname is null or custprofession is null").show()
clean_df_dedup_sql_fill_na.where("id is null or custlname is null or custprofession is null").show()
clean_df_dedup_sql_fill_na.createOrReplaceTempView("clean_df_dedup_sql_fill_na")
spark.sql("select * from clean_df_dedup_sql_fill_na").show()
clean_df_drop_key_null_fill_na_replace_sql = spark.sql('''select id, custfname, custlname, case custprofession 
                                                when "Therapist" then "Physician"
                                                when "Musician" then "Music Director"
                                                when "NA" then "prof not defined"
                                                else custprofession
                                                end  as custprofession 
                                                from clean_df_dedup_sql_fill_na where id is not null''')
clean_df_drop_key_null_fill_na_replace_sql.show()


print("d.1. Data Standardization (column) - Column re-order/number of columns changes (add/remove/Replacement)  to make it in a usable format")
'''
+-------+---------+---------+-------+--------------------+
|     id|custfname|custlname|custage|      custprofession|
+-------+---------+---------+-------+--------------------+
'''
#DSL Functions to achive reorder/add/removing/replacing respectively select,select/withColumn,select/drop,select/withColumn
#select - reorder/add/removing/replacement/rename
#withColumn - add
#withColumn - Replacement of columns custlname with custfname
#withColumnRenamed - rename of a given column with some other column name
#drop-remove columns

#select - reorder/add/removing/replacement/rename
# we are removing custfname and rename custlname as custfname and rename custage as age and add one new called sourcesystem
# with value retailsystem
clean_df_drop_key_null_fill_na_replace.select('id', col('custlname').alias("age"),'custage','custprofession',lit('retailsystem')).show()
'''
+-------+--------+-------+--------------------+------------+
|     id|     age|custage|      custprofession|retailsystem|
+-------+--------+-------+--------------------+------------+
|4000943| Burnett|     72|         Firefighter|retailsystem|
'''
# with value retailsystem and column name as sourcesystem instead of retailsystem

reord_add_rem_repl_ren_df1 = clean_df_drop_key_null_fill_na_replace.select('id', col('custlname').alias("age"),'custage','custprofession',lit('retailsystem').alias('sourcesystem'))
reord_add_rem_repl_ren_df1.show()

# try reordering for reordering select is the best option
reord_df2 = clean_df_drop_key_null_fill_na_replace.select("id", "custprofession", "custage", "custlname", "custfname")
reord_df2.show()

# for adding new column we usually don't use select instead we use with column
# lets acheive the sam eusing withColumn

source = 'retail'
# reord_added_df3 = reord_df2.withColumn(colName="source", Col=source)  -- wrong declaration
# we have to use lit whenever we use constants
from pyspark.sql.functions import lit

# reord_added_df3 = reord_df2.withColumn(colName="source", lit('retail'))
# if we passs one position as postional arguement, then rest also should pass the same else u ll get errro says
# SyntaxError: positional argument follows keyword argument
reord_added_df3 = reord_df2.withColumn(colName="source", col=lit(source))
# or like this u can give
reord_added_df3 = reord_df2.withColumn("source", lit('retail'))
reord_added_df3.show()

#replacement of column(s)
# we are interchanging values of custlname to custfname make as fname and lname as upper of firstname
'''
+-------+--------------------+-------+---------+---------+------+
|     id|      custprofession|custage|custlname|custfname|source|
+-------+--------------------+-------+---------+---------+------+
|4000943|         Firefighter|     72|  Burnett|     Luis|retail|
|4001069|      Police officer|     28|   Davies| Jonathan|retail|
|4001244| Automotive mechanic|     38|  Whitley|   Ernest|retail|
'''
reord_added_replaced_df4 = reord_added_df3.withColumn("custfname", col("custlname"))  # preffered way if few columns requires drop
'''
+-------+--------------------+-------+---------+---------+------+
|     id|      custprofession|custage|custlname|custfname|source|
+-------+--------------------+-------+---------+---------+------+
|4000943|         Firefighter|     72|  Burnett|  Burnett|retail|
|4001069|      Police officer|     28|   Davies|   Davies|retail|
|4001244| Automotive mechanic|     38|  Whitley|  Whitley|retail|
'''

#we can do rename/duplicating column/derivation of a column also using withColumn, we will see further down
reord_added_replaced_upper_df = reord_added_replaced_df4.withColumnRenamed("custlname", "uppercustfname").\
    select("id", "custprofession", col("custage").alias("age"), upper("uppercustfname").alias("Ucase_custfname"), "custfname", "source")
reord_added_replaced_drop_df = reord_added_replaced_df4.drop("custlname")
reord_added_replaced_drop_df1 = reord_added_replaced_drop_df.select("*", upper("custfname").alias("Ucustfname"))


#wih Column rename

reord_added_replaced_rename_df = clean_df_drop_key_null_fill_na_replace.withColumnRenamed("custlname", "custfname").withColumnRenamed("custage", "age")
reord_added_replaced_rename_concat_df = reord_added_replaced_rename_df.select(col("custfname").alias("old"))
reord_added_replaced_rename_df = clean_df_drop_key_null_fill_na_replace.drop("custfname").withColumnRenamed("custlname", "custfname").withColumnRenamed("custage", "age")
reord_added_replaced_rename_tst_df = clean_df_drop_key_null_fill_na_replace.drop("custlname").withColumnRenamed("custlname", "custfname").withColumnRenamed("custage", "age")
reord_added_replaced_upper_df1 = reord_added_replaced_upper_df.drop("custlname").withColumnRenamed("custlname", "custfname").withColumnRenamed("custage", "age")
reord_added_replaced_upper_df1.show()

clean_df_drop_key_null_fill_na_replace_sql.createOrReplaceTempView("clean_df_drop_key_null_fill_na_replace_sql")
reord_added_replaced_drop_sql = spark.sql("select id, custfname,custlname,custprofession, 'retail' from clean_df_drop_key_null_fill_na_replace_sql")
reord_added_replaced_drop_sql = spark.sql("select id, custlname custfname,custprofession, 'retail' source from clean_df_drop_key_null_fill_na_replace_sql")
reord_added_replaced_drop_sql.show()

print("********************data munging completed****************")
munged_df = reord_added_replaced_upper_df1
#select,drop,withColumn,withColumnRenamed
# We have added those as part of Data mungling also but we are doing this for enrichment - to bring more cols
#Adding of columns (withColumn/select) - for enriching the data

# Let's add  curr date and load time
enrich_df1 = munged_df.withColumn("curr_date", current_date()).withColumn("load_time",current_timestamp())
# or we can acheive using select
enrich_df1 = munged_df.select("*", current_date().alias("curr_date"), current_timestamp().alias("load_time"))
enrich_df1.show()

# we can add a sequence number also as a column and we can callit as surrogate key
munged_df.withColumn("skey", monotonically_increasing_id()).show()

#Rename of columns (withColumnRenamed/select/withColumn & drop) - for enriching the data
enrich_df2 = enrich_df1.withColumnRenamed("source","srcsystem")

#Concat to combine/merge/melting the columns
enrich_df3 = enrich_df2.select("id", "curr_date", concat("custfname" + lit(" is a ") + "custprofession").alias("nameprof"), "age", "srcsystem", "load_time")
#wrong declaration, we should use , instead of +
enrich_df3 = enrich_df2.select("id", "curr_date", concat("custfname" , lit(" is a ") , "custprofession").alias("nameprof"), "age", "srcsystem", "load_time")

#Splitting of Columns to derive custfname
enrich_df4 = enrich_df3.select("*",split("nameprof","is a")[0].alias("custfname"))
enrich_df4.show(10, False)

#Casting of Fields
enrich_df4.printSchema()
enrich_df5 = enrich_df4.withColumn("currdt_str",col("curr_date").cast("string")).withColumn("year",year("curr_date")).withColumn("month", month("curr_date")).withColumn("day_str",substring("currdt_str",9,2))

#Reformat same column value or introduce a new column by reformatting an existing column (withcolumn)
enrich_reformat_df5 = enrich_df4.withColumn("currdt_str", col("curr_date").cast("string")).withColumn("year", year("curr_date")).withColumn("currdt_str",concat(substring("currdt_str",3,2),lit("/"),substring("currdt_str",6,2)))
enrich_reformat_df5.show(10, False)

#ansi SQL
#munged_df to enrich_reformat_df5
#SQL equivalent using inline view/from clause subquery

munged_df.createOrReplaceTempView("munged_view")
spark.sql("select * from munged_view").show()
enrich_reformat_sql = spark.sql("""select id, curr_date,  nameprof, age, srcsystem, load_time, custfname, 
                    concat(substring(currdt_str,3,2),'/',substring(currdt_str,6,2)) as currdt_str, year(curr_date) as year
                    from (select id, current_date() as curr_date, concat(custfname, ' is a ', custprofession) as nameprof, age,
                    source as srcsystem, current_timestamp() as load_time, custfname, cast(current_date() as string) as currdt_str 
                    from  munged_view)temp""")
enrich_reformat_sql.show(10, False)

#****************Data Enrichment Completed Here*************#

print("***************3. Data Customization & Processing (Business logics) -> Apply User defined functions and utils/functions/modularization/reusable functions & reusable framework creation *********************")

munged_enriched_df = enrich_reformat_df5

# UDF - we have to create UDF only it is inevitable

convertUpperUDF_lam=lambda prof:prof.upper()

convertUpperUDF_df = udf(convertUpperUDF_lam)                       # for DSL
spark.udf.register("convertUpperUDF_sql", convertUpperUDF_lam)      # for SQl

munged_enriched_customize_df = munged_enriched_df.withColumn("custfname_upper", convertUpperUDF_df("custfname"))
munged_enriched_customize_df.show(10, False)

enrich_reformat_df5.createOrReplaceTempView("enrich_reformat_view")
spark.sql("select convertUpperUDF_sql(custfname) from enrich_reformat_view").show()
spark.sql("select convertUpperUDF_sql(custfname) from enrich_reformat_view").show()
#In the above case, usage of built in function is better - built in is preferred, if not available go with UDF

#Quick Usecase:
#Write a python def function to calculate the age grouping/categorization of the people based on the custage column,
# if age<13 - childrens, if 13 to 18 - teen, above 18 - adults
#derive a new column called age group in the above dataframe (using DSL)
#
def age_validation(age):
    if age < 13:
        return "Childer"
    elif age <= 19:
        return "teen"
    else:
        return "Adults"


udf_custom_validation = udf(age_validation)    # for DSL
spark.udf.register("age_validation_sql", age_validation)
munged_enriched_df1 = munged_enriched_df.withColumn("age_group",udf_custom_validation("age"))
munged_enriched_df2 = munged_enriched_df1.withColumn("age_group",when(col("age")<13,lit("Children")).when(col("age")<=18,lit("Teen")).otherwise(lit("Adults")))
munged_enriched_df2.show(10, False)

#Marry DSL & SQL - Writing SQL expressions in DSL
# without creating a temp view
# by using expr/selectExpr, we can use sql on top of df right or sql along with dsl
munged_enriched_df3 = munged_enriched_df.selectExpr("*", "case when age <= 13 then 'Childeren' when age <=19 then 'teen' else 'Adults' end as Age_group")
munged_enriched_df3 = munged_enriched_df.select("*", expr("case when age <= 13 then 'Childeren' when age <=19 then 'teen' else 'Adults' end as Age_group"))
munged_enriched_df3.show()

#Step6: Display the dataframe and Filter the records based on age_group
munged_enriched_customize_df = munged_enriched_df3.filter("Age_group = 'Adults'")
munged_enriched_customize_df.show()

#MUST GO function - Below utils/functions/modularization/reusable framework are suggested to Standardize the code


def convert_file_to_df(ses,de,path,md, hdr):
    df = spark.read.option("delimiter", de).csv(path, mode=md, header= hdr)
    return df


df1 = convert_file_to_df(spark, "~", "E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv", 'permissive', True)
df1.show()

#Conclusion of Data Customization:
#1. Avoid usage of udfs if it is inevitable
#2. If udf has to be used in dsl - convert python func into udf
#3. If udf has to be used in sql - convert and register python func into udf
#4. If udf is avoidable, write custom functionality using DSL/SQL/DSL+SQL (using expr/selectExpr)
#5. utils/functions/modularization/reusable framework - Suggested to create Functions and reuse them eg. convertFileToDF


print("***************4. Core Data Processing/Transformation (Level1) (Pre Wrangling) Curation -> "
"filter, transformation, Grouping, Aggregation/Summarization, Analysis/Analytics *********************")

prewangled_munged_enriched_customize_df = munged_enriched_customize_df.withColumn("custprofession", split("nameprof", " is a ")[1])
prewangled_munged_enriched_customize_df = munged_enriched_customize_df.select("*", split("nameprof", ' ')[3].alias("custprofession"))
prewangled_munged_enriched_customize_df.show()
#pre wrangled data to consume in the next wrangling stage
#pre wrangled data to produce it to the consumer last persistant stage
#DSL:
prewangled_munged_enriched_customize_df1 = prewangled_munged_enriched_customize_df.\
    select("id","custprofession","age","srcsystem","curr_date").\
    where("upper(custprofession) == 'WRITER' or upper(custprofession) == 'POLICE'").\
    groupby("custprofession","age").agg(min(col("age").cast("string")).alias("min_age")).\
    where("min_age > 33")

#You cannot use show() on a GroupedData object without using an aggregate function (such as sum() or even count()) on it before.

filtered_nochildren_rowcol_df_for_further_wrangling1 = munged_enriched_customize_df.filter("Age_group<>'Children'").select("id","age","curr_date","custfname","year","Age_group")  #  Will be used in the Data persistant last stage
filtered_nochildren_rowcol_df_for_further_wrangling1.show()
#.agg(avg("age").alias("avg_age"), max("age").alias("highest age")).where("avg_age > 42").\
#    orderBy("age")
prewangled_munged_enriched_customize_df1 = prewangled_munged_enriched_customize_df.\
    select("custprofession","id","age","srcsystem","curr_date").\
    where("upper(custprofession) == 'WRITER' or upper(custprofession) == 'POLICE'").\
    groupby("custprofession").agg(count("custprofession").alias("number_of_prof")).\
    where("number_of_prof > 0").orderBy("custprofession")
prewangled_munged_enriched_customize_df2 = prewangled_munged_enriched_customize_df.\
    select("custprofession","id","age","srcsystem","curr_date").\
    where("upper(custprofession) == 'WRITER' or upper(custprofession) == 'POLICE'").orderBy("age")
prewangled_munged_enriched_customize_df.select("custprofession").distinct().show(10, False)
prewangled_munged_enriched_customize_df.select("custprofession", "id").groupby("custprofession").\
    agg(count("id").alias("no_of_persons")).orderBy(col("no_of_persons").desc()).show(10, False)
prewangled_munged_enriched_customize_df3 = prewangled_munged_enriched_customize_df.select("custprofession", "id").\
    where("upper(custprofession) == 'WRITER' or upper(custprofession) == 'POLICE'").groupby("custprofession").\
    agg(count("id").alias("no_of_persons")).where("no_of_persons > 200").\
    orderBy(col("no_of_persons").asc())
prewangled_munged_enriched_customize_df3.show(10, False)


prewangled_munged_enriched_customize_df1.show(10, False)
#SQL:
prewangled_munged_enriched_customize_df.createOrReplaceTempView("prewangled_munged_enriched_customize_view")
prewangled_munged_enriched_customize_sql = spark.sql("""select custprofession, count(id) as no_of_persons from
prewangled_munged_enriched_customize_view group by custprofession having no_of_persons > 250 order by no_of_persons desc
""")
prewangled_munged_enriched_customize_sql.show(10, False)

#Dimensions & Measures we are going to derive
#Dimension - multiple view or viewing of data in different aspects/factors (agegroup,year,profession wise)
#Measures(number based)/Metrics - It is a factual value/metrics/key performance indicator

custom_munged_enriched_df = munged_enriched_customize_df #.filter("Age_group = 'Adults'")
custom_munged_enriched_df1 = custom_munged_enriched_df.withColumn("cusprofession", split("nameprof"," ")[3])
custom_munged_enriched_df1.show(10, False)

#Tell me year,agegroup,profession wise max/min/avg/count/distinccount/mean of age and available date of the given dataset

#countDistinct() - let say if we take all the columns then the entire row will trate as duplicates
#                   with countdistinct we can give selected columns and it will return distinct records for those given cols only

'''

+-------------+----------+------+
|employee_name|department|salary|
+-------------+----------+------+
|        James|     Sales|  3000|
|      Michael|     Sales|  4600|
|       Robert|     Sales|  4100|
|        Maria|   Finance|  3000|
|        James|     Sales|  3000|
|        Scott|   Finance|  3300|
|          Jen|   Finance|  3900|
|         Jeff| Marketing|  3000|
|        Kumar| Marketing|  2000|
|         Saif|     Sales|  4100|
+-------------+----------+------+

# if we put distinct count we will be getting 9 but if we put countdistinct(salary, depart) we will getting count as 8 only

'''
custom_munged_enriched_df2 = custom_munged_enriched_df1.groupby("year", "Age_group", "cusprofession").agg(max("age").alias("max_age"), min("age").alias("min_age"),\
avg("age").alias("Avg_age"), countDistinct("age").alias("count_age")).orderBy("year", "Age_group", "cusprofession",ascending=[True, False, True]).where("Avg_age > 35")

#Tell me the average age of the above customer is >35 (adding having)

#Analytical Functionalities
#Data Random Sampling:

randomsample_custom_munged_enriched_df2 = custom_munged_enriched_df2.sample(0.2,10)
randomsample_custom_munged_enriched_df2.show(10, False)

#summary
custom_munged_enriched_df2.summary().show(10, False)
'''
>>> custom_munged_enriched_df2.summary().show(10, False)
+-------+------+---------+-------------+-----------------+------------------+------------------+------------------+
|summary|year  |Age_group|cusprofession|max_age          |min_age           |Avg_age           |count_age         |
+-------+------+---------+-------------+-----------------+------------------+------------------+------------------+
|count  |50    |50       |50           |50               |50                |50                |50                |
|mean   |2023.0|null     |null         |74.26            |21.26             |48.286383927813276|50.98             |
|stddev |0.0   |null     |null         |4.818374694217705|1.2906255404703126|2.1369592488298617|7.4408963690882155|
|min    |2023  |Adults   |Accountant   |41               |21                |35.5              |2                 |
|25%    |2023  |null     |null         |75               |21                |47.81730769230769 |51                |
|50%    |2023  |null     |null         |75               |21                |48.481675392670155|52                |
|75%    |2023  |null     |null         |75               |21                |49.29953198127925 |54                |
|max    |2023  |Adults   |prof         |77               |30                |50.578947368421055|55                |
+-------+------+---------+-------------+-----------------+------------------+------------------+------------------+
'''
custom_munged_enriched_df2.cov("max_age", "min_age")  #-6.048571428571428
custom_munged_enriched_df2.corr("max_age", "min_age")  #-0.9726396934936626
custom_munged_enriched_df2.freqItems(['year'], .4).show(10, False) #[2023]

## SQL

# We have to recreate custom_munged_enriched_df2 with equivalent sql from the custom_munged_enriched_df1
custom_munged_enriched_df1.show()
custom_munged_enriched_df1.createOrReplaceTempView("custom_munged_enriched_sql")
dim_year_agegrp_prof_metrics_avg_mean_max_min_distinctCount_count_for_consumption2_sql1
spark.sql("""select year, Age_group, cusprofession, max(age) max_age, min(age) min_age, avg(age) avg_age, 
count(distinct age) count_age from custom_munged_enriched_sql group by year, Age_group, cusprofession 
having avg_age > 35 order by year, Age_group, cusprofession""").show(10, False)

# if we are not using having in the above sql, then we have to create a subquery and then with where cluse to acheive it


print("***************5. Core Data Curation/Processing/Transformation (Level2)")
# Joins, Lookup, Lookup & Enrichment, Denormalization,Windowing, Analytical, set operations, Summarization (joined/lookup/enriched/denormalized) *********************")

txns_schema = StructType([StructField("Sno", StringType(), False),
                          StructField("cob_date", StringType(), False),
                          StructField("Acc_no", IntegerType(), False),
                          StructField("Amnt", FloatType(), False),
                          StructField("Catgry", StringType(), True),
                          StructField("Product", StringType(), True),
                          StructField("City", StringType(), True),
                          StructField("State", StringType(), True),
                          StructField("Transaction", StringType(), True)])
txns = spark.read.option("inferschema", True).option("delimiter",",").option("header",False).schema(txns_schema).csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\txns").toDF("txnid", "cob_date", "custid", "amt", "category", "product", "city", "state", "transtype").na.drop("all")
# we can apply Data munging
# #this data is also eligible for all above DE stages like munging, enrichment, customization, pre wrangling, wrangling,
txns.printSchema()
#Enrichment stage is must needed-
#Wanted to extract the transaction year from the dt column

# we worked how to convert date type date field to string and as part of enrichment lets try reverse string to date type
txns_df = txns.withColumn("cob_date", to_date("cob_date", "MM-dd-yyyy")).withColumn("year", year("cob_date"))
print("Conversion & Formatting functions - Date Transformation")

# txns_dtfmt = txns_df.withColumn("dt_chng_frmt", date_format("cob_date", "MM-dd-yyyy").cast("date"))
txns_dtfmt = txns_df.withColumn("dt_chng_frmt", date_format("cob_date", "MM-dd-yyyy"))
enrich_txns_dfmts = txns_dtfmt.withColumn("month", month("cob_date")).withColumn("days_from_year", dayofyear("cob_date"))\
          .withColumn("days_from_month", dayofmonth("cob_date")).withColumn("days_from_week", dayofweek("cob_date"))\
          .withColumn("next_monday", next_day("cob_date", 'monday')).withColumn("last_day", last_day("cob_date"))\
          .withColumn("15_days_added", date_add("cob_date", 15)).withColumn("7_days_sub", date_sub("cob_date", 7))\
          .withColumn("dayofweek", dayofweek("cob_date")).withColumn("dayofyear", dayofyear("cob_date"))\
          .withColumn("datediff", datediff(current_date(), "cob_date")).withColumn("dt_trunc", trunc("cob_date", "Month"))\
          .select("custid", "cob_date", "year", "month", "days_from_year", "days_from_month", "days_from_week",  "15_days_added", "7_days_sub", "dayofweek", "dayofyear", "datediff", "dt_trunc")
enrich_txns_dfmts.show()
txns_dtfmt.printSchema()

#Enrichment Stage is completed for transaction data


#Joins (Interview Question)
# I will give you 2 datasets, 1 with 14 rows another with 13 rows and common data is 11, what is the count on different joins
#inner join   11
#left join   14
#right join   13
#cross join   182
#full join   27 - 11 = 16
print("Just learn about Joins with examples")

#Connecting the tables using some common fields -> inner join,left join,right join, full outer, natural join, cross join, self join, anti joins, semi joins
#inner join,left join,right join - (natural joinS) - Regularly used joins
#full outer, self join - Rarely used joins
#anti joins, semi joins - Very Rarely used joins
#cross join/cartisian join - Join to be avoided and used only if it is inevitable

cust_df=filtered_nochildren_rowcol_df_for_further_wrangling1

cust_df.show()
enrich_txns_dfmts.show()
#DSL Join syntax (Hard relative than SQL syntax)

cust_df.join(enrich_txns_dfmts, col("id") == col("custid"), "inner").show()

dim_df1 = cust_df.where("id in (4000001,4000002)").select("id","age","custfname","Age_group",lit("cust_lft_tbl").alias("left_tbl_col"))
dim_df2 = cust_df.where("id in (4000001,4000003,4000004)").select("id","age","custfname","Age_group",lit("cust_right_tbl").alias("right_tbl_col"))
dim_df1.show()
dim_df2.show()

# if we didnt mention on and how to thejoj thwn it would become cross join
dim_df1.join(dim_df2).show()#cross join

#If we mention atleast the one on condition then it equi or inner join both are same
dim_df1.alias("t1").join(dim_df2.alias("t2"), col("t1.id")==col("t2.id")).select("t1.id", "t1.age", "t1.custfname","t1.Age_group", "left_tbl_col", "t2.id", "t2.age", "t2.custfname", "t2.Age_group", "righ_tbl_col").show()
dim_df1.alias("t1").join(dim_df2.alias("t2"), col("t1.id")==col("t2.id"), "left").select("t1.id", "t1.age", "t1.custfname","t1.Age_group", "left_tbl_col", "t2.id", "t2.age", "t2.custfname", "t2.Age_group", "righ_tbl_col").show()
dim_df1.alias("t1").join(dim_df2.alias("t2"), col("t1.id")==col("t2.id"), "Right").select("t1.id", "t1.age", "t1.custfname","t1.Age_group", "left_tbl_col", "t2.id", "t2.age", "t2.custfname", "t2.Age_group", "righ_tbl_col").show()
# if we are using id alone in he joining condition since both the table has same cols
dim_df1.alias("t1").join(dim_df2.alias("t2"), on="id").select("t1.id", "t1.age", "t1.custfname","t1.Age_group", "left_tbl_col", "t2.age", "t2.custfname", "t2.Age_group", "righ_tbl_col").show()

#How to find the common columns in a dataframe
set(dim_df1.columns).intersection(set(dim_df1.columns))

cross_join_df = dim_df1.join(dim_df2)
inner_join_df = dim_df1.alias("t1").join(dim_df2.alias("t2"), on=[col("t1.id")==col("t2.id")], how="inner").select("t1.*", "t2.*")
left_outer_df = dim_df1.alias("t1").join(dim_df2.alias("t2"), on=[col("t1.id")==col("t2.id")], how="left").select("t1.*", "t2.*")
right_outer_df = dim_df1.alias("t1").join(dim_df2.alias("t2"), on=[col("t1.id")==col("t2.id")], how="right").select("t1.*", "t2.*")
#Just to check/lookup the records from first table exist in the second table and display the only firs
semi_join_df = dim_df1.alias("t1").join(dim_df2.alias("t2"), on=[col("t1.id")==col("t2.id")], how="semi").select("t1.*")
#It extracts only the non matched data from only the left table, by comparing with the right table and display only left
anti_join_df = dim_df2.alias("t1").join(dim_df1.alias("t2"), on=[col("t1.id")==col("t2.id")], how="anti").select("t1.*")
anti_join_df.show()
###Learning of Joins in Spark SQL (DSL/SQL) is completed here...


print("****************** Started performing 5. Wrangling from here************")
#(Level2) Data Wrangling -> Joins, Lookup, Lookup & Enrichment,
print("a. Lookup (Joins)")
#lookup is an activity of identifying some existing data with the help of new data
#lookup can be achived using joins (semijoin (preferrably), antijoin(preferable for anti lookup), leftjoin, innerjoin)
cust_3custs = custom_munged_enriched_df1.where("id in (4000000,4000001,4000002)")
txns_3custs = txns.where("custid in (4000000,4000001)")
cust_3custs.show()
txns_3custs.show()
#I did a lookup of howmany customer in my customer database have did transaction today
#Returns the details of the customers who did transactions, no transaction details will be provided
cust_3custs.join(txns_3custs, col("id")==col("custid"), "semi").show()

# we can also acheive this using left join and inner join lets try those two
cust_3custs.alias("t1").join(txns_3custs, col("id")==col("custid"), "left").where("custid is not null").select("t1.*").dropDuplicates().show()
cust_3custs.alias("t1").join(txns_3custs, col("id")==col("custid"), "inner").select("t1.*").dropDuplicates().show()

#we can acheive the same bin sql
#join or in which is better - join is better because of different types available

print("b. Lookup & Enrichment (Joins)")
#lookup is an activity of enriching the new data with the existing data
#Returns the details of the customers who did transactions or didn't do the transactions along with what transactions details or null values

cust_3custs.alias("t1").join(txns_3custs, col("id")==col("custid"), "left").show()

#how many customers didn't do transaction today
anti_join = cust_3custs.join(txns_3custs, col("id")==col("custid"), "anti")
anti_join.count()
anti_join.show()
#how many customers did transaction or didn't do the transaction for how much - to see both
left_outer_enrich = cust_3custs.join(txns_3custs, col("id")==col("custid"),"left").groupby("id").agg(count("amt").alias("No of transactions"),sum("amt").alias("Total amount of transactions"))
#didn't do the transaction
anti_enrich = cust_3custs.join(txns_3custs, col("id")==col("custid"), "anti")
#how many customers did transaction for how much
left_enrich = cust_3custs.join(txns_3custs, col("id")==col("custid"),"left").where("custid is not null").groupby("id").agg(count("amt").alias("No of transactions"),sum("amt").alias("Total amount of transactions"))
inner_enrich = cust_3custs.join(txns_3custs, col("id")==col("custid"),"inner").groupby("id").agg(count("amt").alias("No of transactions"),sum("amt").alias("Total amount of transactions"))

print("Lookup and Enrichment Scenario using SQL")
cust_3custs.createOrReplaceTempView("cust_df_view")
txns_3custs.createOrReplaceTempView("enriched_dt_txns_view")

spark.sql("select * from cust_df_view").show(10)
spark.sql("select * from enriched_dt_txns_view").show(10)
# Inner join
spark.sql("""select * from cust_df_view join enriched_dt_txns_view on id == custid""").show()
# left join
spark.sql("""select * from cust_df_view left join enriched_dt_txns_view on id == custid""").show()
# right join
spark.sql("""select * from cust_df_view rght join enriched_dt_txns_view on id == custid""").count()
# full join
spark.sql("""select * from cust_df_view full outer join enriched_dt_txns_view on id == custid""").show()
# semi join
spark.sql("""select * from cust_df_view semi join enriched_dt_txns_view on id == custid""").show()
# Anti Join
spark.sql("""select * from cust_df_view left anti join enriched_dt_txns_view on id == custid""").show()


print("Identify the count of Active Customers or Dormant Customers")

#take latest 1 month worth of txns data and compare with the customer data
txns_dtfmt.groupby("cob_date").show() # if you want distinct dates alone use distinct istead of group by
txns_dtfmt.groupby("cob_date").agg(count("*")).show()
txns_dtfmt.select("cob_date").distinct().orderBy("cob_date", ascending=True).limit(31)
# easy way to get latest one month data is
txns_1month = txns_dtfmt.select("*", month("cob_date").alias("month")).where("year == 2011 and month == 12").drop("month")

#Active Customers (customers did transactions in last 1 month)
Active_customers = custom_munged_enriched_df1.join(txns_1month, col("id")==col("custid"), "semi")
#Dormant Customers (customers didn't do transactions in last 1 month)
Dormant_customers = custom_munged_enriched_df1.join(txns_1month, col("id")==col("custid"), "anti")

#SQL
txns_dtfmt.createOrReplaceTempView("txns_dtfmt_view")
custom_munged_enriched_df1.createOrReplaceTempView("cust_view")
txns_1month_sql = spark.sql("""select * from txns_dtfmt_view where month(cob_date) == 12 and year = 2011""")
txns_1month_sql.createOrReplaceTempView("txns_1month_view")
Active_customers_sql=spark.sql("""select * from cust_view left semi join txns_1month_view on id == custid""")
Dormant_customers_sql=spark.sql("""select * from cust_view left anti join txns_1month_view on id == custid""")
Active_customers_sql.show()
Active_customers.show()
Dormant_customers_sql.show()
Dormant_customers.show()

print("c. (Star Schema model-Normalized) Join Scenario  to provide DENORMALIZATION view or flattened or wide tables or fat tables view of data to the business")
cust_dim = custom_munged_enriched_df1
txns_fact = txns_dtfmt

# As part of this technique, we are joining  the data once for all so that next time we don't need to join every time for analysis
# drawbck is storage space and we storing data again in the joined flatened view

flattened_denormalized_trans_cust_persist = cust_dim.join(txns_fact, col("id")==col("custid"), "inner")
#"Equivalent SQL for Denormalization (denormalization helps to create a single view for faster query execution without joining)")
flattened_denormalized_trans_cust_persist.createOrReplaceTempView("denormalized_view")

print("d. Windowing Functionalities")
#row_number(), rank(), dense_rank(), lead, lag
txns_dtfmt=txns.withColumn("cob_date",to_date("cob_date",'MM-dd-yyyy'))
txns_3custs=txns_dtfmt.where("custid in (4000000,4000001)")
txns_dtfmt.show()
txns_3custs.show()
from pyspark.sql.window import Window

print("aa.How to generate seq or surrogate key column on the ENTIRE data sorted based on the transaction date")
txns_dtfmt.withColumn("surrogate key", monotonically_increasing_id()).show()
txns_dtfmt.select(lit(monotonically_increasing_id()).alias("surrogate key"), "*",).show()
txns_dtfmt.select(lit(substring("cob_date",1,4)).alias("year"), "*").show()
# row_number()
txns_dtfmt.select(row_number().over(Window.orderBy("cob_date")).alias("Row_no"), '*').show()
##row_number will coalesce the number of partition to 1 and it will generate sequence in a right sequence without jumping values

print("bb.How to generate seq or surrogate key column accross the CUSTOMERs data sorted based on the transaction date")
txns_3custs.sort("cob_date", ascending=True)
txns_rowno_order = txns_3custs.select("*", row_number().over(Window.partitionBy("custid").orderBy("cob_date")).alias("trans_order"))


print("first, nth made by a given customer")
txns_nth_1st = txns_rowno_order.select("*").groupby("custid").agg(min("trans_order").alias("min"), max("trans_order").alias("max"))
txns_rowno_order.where("trans_order == 1").show()
txns_rowno_order_lst = txns_3custs.select("*", row_number().over(Window.partitionBy("custid").orderBy(desc("cob_date"))).alias("trans_order"))
txns_rowno_order_lst.where("trans_order == 1").show()

print("Least 3 transactions in our overall transactions")

least_3_transactions = txns_3custs.select("*", row_number().over(Window.orderBy(asc("amt"))).alias("least_trans_order"))
least_3_transactions.show()
print("Least 3 transactions in our overall transactions on cusomer level")
least_3_transactions_cust = txns_3custs.select("*", row_number().over(Window.partitionBy("custid").orderBy("amt")).alias("least_trans_order"))
least_3_transactions_cust.show()
Top_3_transactions_cust = txns_3custs.select("*", row_number().over(Window.partitionBy("custid").orderBy(desc("amt"))).alias("top_trans_order"))
Top_3_transactions_cust.where("custid=4000000 and top_trans_order < 4").show()

dense_rank_df = txns_3custs.select("*", rank().over(Window.partitionBy("custid").orderBy(desc("amt"))).alias("rank_no"))
dense_rank_df.distinct().count()

dense_rank_df.subtract(least_3_transactions).show()
transamt_order_txns3=txns_3custs.select("*",row_number().over(Window.partitionBy("custid","category").orderBy(desc("cob_date"))).alias("transamtorder"))
transamt_order_txns3.where("transamtorder=1").show(22)

# By adding more columns in the partition we can deduplicates the records by row number as 1

print("E. Analytical Functionalities")

print("What is the purchase patter of the given customer, whether his buying potential is increased or decreased transaction by transaction")
#lag
lead_transamt_order_txns3 = txns_3custs.withColumn("next_amt", lead("amt", 1, "start value").over(Window.partitionBy("custid").orderBy("cob_date")))
lead_transamt_order_txns3.show()
lag_transamt_order_txns3 = txns_3custs.withColumn("prior_amt", lag("amt", 1, 0).over(Window.partitionBy("custid").orderBy("cob_date")))
lag_transamt_order_txns3.show()

cust_purchase_patterndf = lag_transamt_order_txns3.select("*",when(col("prior_amt") > col("amt"), lit("Decreased"))
                .when(col("prior_amt")==0, lit("Starting price")).when(col("prior_amt")<col("amt"), lit("Increased")).otherwise(lit("No Change")).alias("pattern"))
cust_purchase_patterndf.show()

print("d. Windowing Functionalities using SPARK SQL")
# Equivalent SQL
txns_3custs.createOrReplaceTempView("txns_3custs_view")
print("aa.How to generate seq or surrogate key column on the ENTIRE data sorted based on the transaction date")
spark.sql("""select *, row_number() over(order by cob_date) as r_no from txns_3custs_view""").show()
print("bb.How to generate seq or surrogate key column accross the CUSTOMERs data sorted based on the transaction date")
spark.sql("""select *, row_number() over(partition by custid order by cob_date) as r_no from txns_3custs_view""").show()
print("last, penultimate transaction by a given customer")
spark.sql("""select * from(select *, row_number() over(partition by custid order by cob_date desc) as r_no from txns_3custs_view
)temp where r_no = 1""").show()
print("Least 3 transactions in our overall transactions")
spark.sql("""select * from(select *, row_number() over(order by amt) as r_no from txns_3custs_view)temp where r_no <4""").show()
print("Top 3 transactions in our overall transactions")
spark.sql("""select * from(select *, row_number() over(order by amt desc) as r_no from txns_3custs_view)temp where r_no <4""").show()
print("Top 3 transactions made by the given customer")
spark.sql("""select * from(select *, row_number() over(partition by custid order by amt desc) as r_no from txns_3custs_view)temp where r_no <4""").show()
print("Top transaction amount made by the given customer 4000000")
spark.sql("""select * from(select *, row_number() over(partition by custid order by amt desc) as r_no from txns_3custs_view)temp where r_no <4 and custid = '4000000'""").show()
print("Top 2nd transaction amount made by the given customer 4000000")
spark.sql("""select * from(select *, row_number() over(partition by custid order by amt desc) as r_no from txns_3custs_view)temp where r_no ==2 and custid = '4000000'""").show()
print("How to de-duplicate based on certain fields eg. show me whether the given customer have played a category of game atleast once")
spark.sql("""select * from(select *, row_number() over(partition by custid, category  order by amt desc) as r_no from txns_3custs_view)temp where r_no ==2 and custid = '4000000'""").show()


print("e. Set Operations ")
#Thumb rules : Number, order and datatype of the columns must be same, otherwise unionbyname function you can use.
print("Common customers accross the city, state, stores, products")

df1=txns_3custs.where("custid=4000000")
df2=txns_3custs

df1.union(df2).count() #34
bothdfdatawithoutduplicates=df2.union(df1).distinct().count()  # 22
bothdfcommondata=df2.intersect(df1)
bothdfcommondata.count()                                       # 12
df2subtracteddf1=df2.subtract(df1) # like right outer join in terms appending records instead of joining here
df1subtracteddf2=df1.subtract(df2)
df2subtracteddf1.count()

print("SQL set operations")
df1.createOrReplaceTempView("df1_view")
df2.createOrReplaceTempView("df2_view")
spark.sql("select * from df1_view union all select * from df2_view").show()  # 34
spark.sql("select * from df1_view union select * from df2_view").count()  # 22
spark.sql("select * from df1_view intersect select * from df2_view").count()   # 12
spark.sql("select * from df1_view except select * from df2_view").count()  # 0
spark.sql("select * from df2_view except select * from df1_view").count()  # 10


print("Aggregation on the joined/Windowed/Analysed/PreWrangled data set")
# get the number of transaction and total amount the customers made
analysed_aggr1 = cust_purchase_patterndf.groupby("custid","pattern").agg(count("custid").alias("No of ransaction"), sum("amt").alias("total amount"))
left_outer_enrich.show()
inner_enrich.show()
###########Data processing or Curation or Transformation Starts here###########

print("***************6. Data Persistance (LOAD)-> Discovery, Outbound, Reports, exports, Schema migration  *********************")
#wrangled data
analysed_aggr1.write.mode("overwrite").csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\write\\analysed_aggr1.csv")
#pre-wrangled data
randomsample_custom_munged_enriched_df2.write.mode("overwrite").csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\write\\random_sample.csv")
flattened_denormalized_trans_cust_persist.write.mode("overwrite").csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\write\\denormalised.csv")
masked_custdata=cust_purchase_patterndf.groupBy("custid","pattern").agg(count("custid").alias("custcnt")).select("pattern",md5(col("custid").cast("string")).alias("masked_custid"),"custcnt")
masked_custdata.write.mode("overwrite").csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\write\\masked.csv")
#    saveAsTable("default.cust_masked")
#munged data
custom_munged_enriched_df2.write.mode("overwrite").csv("E:\\BigData\\Shared_Documents\\sample_data\\hive\\write\\munged.csv")
print("Spark App1 Completed Successfully")


