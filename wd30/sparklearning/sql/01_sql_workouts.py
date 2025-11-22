from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import lit, col, sum
from pyspark.sql.types import StructType, StructField, StringType, FloatType



spark = SparkSession.builder.master("local[2]").appName("Spark_sql_learning").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
read_rdd = sc.textFile("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\empdata.txt")
# print(read_rdd.collect())
#read_rdd.foreach(print)
rd_map = read_rdd.map(lambda x: x.split(","))  # it will treat actual end of the line in the record as end of line
# print(rd_map.collect())
rd_fmap = read_rdd.flatMap(lambda x: x.split(","))   # it will treat the first space its encountered as end of line
# print(rd_fmap.collect())
#rd_map.foreach(print)
# rd_fmap.foreach(print)

driver_bonus = 10000
driver_bonus_bc = sc.broadcast(driver_bonus)
rd_cols = rd_map.map(lambda x: (x[0], int(x[2]), int(x[4]) + driver_bonus_bc.value)) # we have to use th e.value option to bring the value from the broadcasted variable
#rd_cols.foreach(print)
sc.setCheckpointDir("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\chkpoint")
# rd_cols.cache()
# rd_cols.checkpoint()
# rd_cols.foreach(print)
# print(rd_cols.collect())
# rd_cols.foreach(print)
# print(rd_cols.getCheckpointFile())

col_list = ["Name", "Age", "Salary"]
rd_cols_df = rd_cols.toDF(col_list)
# rd_cols_df.foreach(print) # Refelection Row(rdd) Row(Name='irfan', Age=37, Salary=20000)
# print(rd_cols_name.collect()) # same  Refelection Row(rdd) Row(Name='irfan', Age=37, Salary=20000
# ########### or ####################
rd_cols_row = rd_cols.map(lambda x: Row(Name=x[0], Age=int(x[1]), Sal=int(x[2])))
# rd_cols_row.foreach(print)
# Row(Name='ArunKumar', Age=33, Sal= 110000)
# Row(Name='vasudevan', Age=43, Sal=100000)

# because we dont use df function we still used rdd releated
#rd_cols_df.show()
# ###   or    ########
rd_cols_cdf = spark.createDataFrame(rd_cols, col_list)
# rd_cols_cdf.show()
rd_cols_df.select("Name", "Salary")
# rd_cols_df.select("Name", "Salary").where("age" > 35) # this representation of where clause is wrong
rd_cols_df.select("Name", "Salary").where('age > 35')
# rd_cols_df.select("Name", "Salary", sum("Salary")).groupBy("age")
df_distinct = rd_cols_df.distinct()
# df_distinct.withColumn("Country", lit("India"), "org_sal", df_distinct.Salary-driver_bonus)
# wont work we can add only one column using withcol at a time
df_add_cols = df_distinct.withColumn("Country", lit("India")).withColumn("org_sal", df_distinct.Salary-driver_bonus)
# .withColumn("bonus_sal", df_distinct.org_sal*0.1)
df_add_col = df_add_cols.withColumn("bonus_sal", df_add_cols.org_sal*0.5)
# df_add_col.show()

# Rather writing the code like above (either schemardd + collist or rowrdd to DF) (create rdd then converting to DF),
# preferable write the code (directly create DFs) as given below

df1 = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\sampledata.txt")  #sampledata is a text file
df2 = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\sampledata.csv")  #sampledata is a csv file
# df1.show()

# df1.printSchema()
# df2.printSchema()

# In here all treated as string, even we have salary in the file,

df1_infer = spark.read.option("inferschema", True).csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\sampledata.txt")
# df1_infer.printSchema()
# now it infer the datatype and given the right value


df1_infer_n = spark.read.option("inferschema", True).option("header", False)\
    .csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\sampledata.txt")
# df1_infer.toDF(["ID", "Name", "Sal"])
# to need the input in List format because the input is not rdd and its df already so
df1_inferred_header = df1_infer.toDF("ID", "Name", "Sal")

df1_header_already = spark.read.options(inferschema=True, header=True)\
    .csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\sampledata.csv")

df1_header_new = df1_header_already.toDF("ID", "Name", "Sal")
df1_header_new.createOrReplaceTempView("sample_view")

# spark.sql("select * from sample_view").show()

# Structtype
'''struct_schema = StructType([StructField("Exchange", StringType(), False), StructField("Stock", StringType(), True),
                            StructField("")])'''
# False -- It can't be null,
# True --  It can be nullable
df_using_csv = spark.read.options(delimiter="~").csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")
df_using_csv.show()
df_using_csv = spark.read.options(delimiter="~", inferschema=True, header=False) \
    .csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")
# df_using_csv.printSchema()
# But we have int or double value in col 3 and we have to assign header to true then since it has header

df_using_csv_format = spark.read.format("csv").options(delimiter="~", inferschema=True, header=True) \
    .csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")
# df_using_csv_format.printSchema()
# and preference 3 if other sources we are going to use . csv,json, orc, jdbc
df_using_txt = spark.read.options(delimiter="~", inferschema=True, header=True)\
    .text("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")
df_using_csv_format_tx = spark.read.format("text").options(delimiter="~", inferschema=True, header=True) \
    .csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")
df_using_csv_format_tx.show()
# It will give as text file as output

# if there is no header
df_no_header = spark.read.options(delimiter="~", inferschema=True, header=True)\
    .csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse_noheader.csv")
df_no_header.show()
df_no_header = spark.read.options(delimiter="~", inferschema=True)\
    .csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse_noheader.csv")

# Now I want to add header
# TO create all the columns or replace most of the column
df_header = df_no_header.toDF("Exchange", "Stock", "Close_Rate")
# Rename the few column may be Stock with Stock_value
df_header = df_header.withColumnRenamed("Stock", "Stock_Value")
df_header.select(col("Exchange").alias("Exchange_Name"), "Stock_Value", "Close_Rate")
# or
# convert df to tempview and write ansi sql
df_header.createOrReplaceTempView("stock_details")
spark.sql("select Exchange as Exchange_Name,Stock_Value from stock_details").show()

# sum of closerate value group by exchange name
df_using_csv_format.createOrReplaceTempView("stock_details")
spark.sql("select exchange as Exchange_Name,stock Stock_Value, sum(closerate) total_rate from stock_details group by exchange,stock").show()
spark.sql("select exchange as Exchange_Name, sum(closerate) total_rate from stock_details group by exchange").show()
# or we can DSL query like below
# df_using_csv_format.groupBy("exchange").select("exchange", sum("closerate").alias("total_rate")) #no neeed of select
df_using_csv_format.groupBy("exchange").sum("closerate").alias("total_rate")
'''
+--------+-----------------+
|exchange|   sum(closerate)|
+--------+-----------------+
|     BSE|602.5999999999999|
|    NYSE|            96.12|
+--------+-----------------+
'''
df_using_csv_format.groupBy("exchange").agg(sum("closerate").alias("total_rate"))
df_using_csv_format.select()
df_using_csv = df_using_csv_format.select("Exchange", "Stock", col("closerate").cast("int"))
df_using_csv.select(sum("closerate"))
df_using_csv.select("closerate").agg(sum("closerate"))
# agg(max("")) here select is not needed

# df_using_csv_format.where("Stock==null").show() -- it won't work
df_using_csv_format.where("stock is null").show()
df_using_csv_format.where("stock=='REL'").show()
df_using_csv_format.where("closerate > 200").show()
df_using_csv_format.printSchema()
'''
root
 |-- exchange: string (nullable = true)
 |-- stock: string (nullable = true)
 |-- closerate: double (nullable = true)
'''

df_json = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse_json")
df_json.show()
df_json = spark.read.json("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse_corrupt.json")
#  By default JSON data source inferschema from an input file.
df_json_single = spark.read.json("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\zipcode.json")
# single line json records

df_json = spark.read.json("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.json")
df_json_multiline = spark.read.json("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\multiline_json.json")
df_json_multiline.show() # error will be there since its multiline records
df_json_multiline = spark.read.option("multiline","true").\
    json("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\multiline_json.json")
'''
+-------------------+------------+-----+-----------+-------+
|               City|RecordNumber|State|ZipCodeType|Zipcode|
+-------------------+------------+-----+-----------+-------+
|PASEO COSTA DEL SUR|           2|   PR|   STANDARD|    704|
|       BDA SAN LUIS|          10|   PR|   STANDARD|    709|
+-------------------+------------+-----+-----------+-------+
'''

# write csv file into json format

df_using_csv_format.write.json("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse_csv_to_json.json")
df_json_read = spark.read.json("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse_csv_to_json.json")
df_json_read.show()
df_using_csv_format.write.saveAsTable("csv_table")
# create the struct type for no header even header type also

df_no_header.show()
col_struct = StructType([StructField("Exchange",StringType(), True),
                         StructField("Stock", StringType(), False),
                         StructField("Close_Rate", FloatType(), False)])

df_struct_type = spark.read.options(col_struct=True)\
    .csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse_noheader.csv")
# the above representation is wrong there is no col_struct as existing keyword

df_struct_type = spark.read.schema(col_struct) \
    .csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse_noheader.csv")

'''
+--------------+-----+----------+
|      Exchange|Stock|Close_Rate|
+--------------+-----+----------+
| NYSE~CLI~35.3| null|      null|
|NYSE~CVH~24.62| null|      null|
| NYSE~CVL~16.2| null|      null|
+--------------+-----+----------+
'''

# we have still to mention delimiter

df_struct_type = spark.read.schema(col_struct).options(delimiter="~")\
    .csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse_noheader.csv")

# Even If header is available, no issues

df_hdr_dflt = spark.read.options(header=True, delimiter="~").csv\
    ("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")
df_struct_hdr = spark.read.options(header=True, delimiter="~", inferschema=True).schema(col_struct)\
    .csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")
df_hdr_infer = spark.read.options(header=True, delimiter="~", inferschema=True).csv\
    ("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")
df_hdr_dflt.printSchema()
df_struct_hdr.printSchema()
df_hdr_infer.printSchema()

rdd1 = sc.textFile("E:\\BigData\\Shared_Documents\\sample_data\\mrdata\\courses.log").flatMap(lambda x: x.split(" "))
course_col = ["Courses"]
df1_new = spark.createDataFrame(rdd1, course_col)
rdd1.toDF(course_col).show(10)
# we are getting error like this, for the above commands,
'''    raise TypeError("Can not infer schema for type: %s" % type(row))
TypeError: Can not infer schema for type: <class 'str'>  '''

#  This is because the flatMap results list of strings, we have to convert to int that is
rdd1_tuple = rdd1.flatMap(lambda x: x.split(" ")).map(lambda x: (x,))
df1_new = spark.createDataFrame(rdd1_tuple, course_col)
df1_new.show()
df1_new.distinct()
df1_new.select("what").distinct().show()
df1_new.select("courses").distinct().show()
df1_new.select("courses").where("courses == 'gcp'").count()

df2=spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv",sep='~',inferSchema=True)
#This time its worked good


struct_cols = StructType([StructField("Stock", StringType(), False),
                          StructField("Exch_Name", StringType(), True),
                          StructField("Price", FloatType(), True)])
df_test_purpose = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse_noheader.csv",
                                 schema=struct_cols, sep="~")
df_test_purpose_hdr = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv",
                                 header=True, schema=struct_cols, sep="~")
#below is wrong declaration when it is option then it would be the below format,
df_test_purpose_infer = spark.read.option("inferschema", True)\
    .option("header", True).csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")

#option("header","true").option("inferschema","true")
#options(header= True, inerschema= True)

df_test_purpose_option = spark.read.option("inferschema","true").option("header", "true").option("delimiter","~")\
    .csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")
df_test_purpose_option_schema = spark.read.option("inferschema","true").option("header", "true").option("delimiter","~")\
    .schema(struct_cols).csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")
df_test_purpose_option.printSchema()
df_test_purpose_option_schema.printSchema()
# We should not give path and all
df_test_purpose_options = spark.read.options(header= True, sep= "~", inferschema= True, path="E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")
df_test_purpose_options_sep = spark.read.options(header= True, sep= "~", inferschema= True)\
    .csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")
df_test_purpose_options_deli = spark.read.options(header= True, delimiter= "~", inferschema= True)\
    .csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")

#we cant give schema here
df_test_purpose_options_schema = spark.read.options(header= True, delimiter= "~", inferschema= True, schema=struct_cols)\
    .csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")
df_test_purpose_options_schemw = spark.read.options(header= True, delimiter= "~", inferschema= True)\
    .csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv").toDF(struct_cols)
'''
df_test_purpose_options_schemr = spark.read.options(header= True, delimiter= "~", inferschema= True)\
    .csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv").schema(struct_cols) '''
#we have to give here
df_test_purpose_options_schem = spark.read.schema(struct_cols).options(header= True, delimiter= "~", inferschema= True)\
    .csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\nyse.csv")

df_read_cols = spark.read.csv("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\txns")
df_read_cols.select("_c0", "_c2", "_c7", "_c8").show()
#wrong declaration of cast
#df_read_cols.select(col("_c0").cast(int).alias("acc_no"), "_c2", col("_c7").alias("State"), "_c8").show()
df_read_cols.select(col("_c0").cast("int").alias("acc_no"), "_c2", col("_c7").alias("State"), "_c8").show()

df_minim = df_read_cols.select(col("_c0").alias("acc_no"), "_c2", col("_c7").alias("State"), "_c8")
struct_df = StructType([StructField("Seq_no",StringType(), False),
                        StructField("Acc_No",StringType(), False),
                        StructField("State",StringType(), False),
                        StructField("Amount", StringType(), False)])
spark.createDataFrame(df_minim,struct_df)
#TypeError: data is already a DataFrame
df_name = spark.createDataFrame(df_minim.rdd,struct_df)

















































