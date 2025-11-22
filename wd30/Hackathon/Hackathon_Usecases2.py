import sys
import os
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def main(arg):
    spark = SparkSession.builder\
       .appName("Hackathon Usecases2") \
       .config("spark.jars", "/home/hduser/install/mysql-connector-java.jar")\
       .enableHiveSupport()\
       .getOrCreate()
    print("Set the logger level to error")
    spark.sparkContext.setLogLevel("ERROR")
    location = arg[1]
    print("Location :" + location)
    sc = spark.sparkContext
    print("1. Please familiarize with the given data set (manuf_data.csv) and take notes of any peculiarities you discover "
          "throughout working on the dataset")
    print("Lets read the data manuf_data.csv to analyze the raw data to identify the attributes and patterns.")
    manu_data_raw_df = spark.read.csv(location + "manuf_data.csv", inferSchema=True, header=True, mode='permissive')
    print(len(manu_data_raw_df.collect()))  # 2519
    manu_data_raw_df.printSchema()
    manu_data_raw_df.show(5, False)
    print("take some sample records using sampling method to analyse the pattern")
    randomsample_manu_data_raw_df = manu_data_raw_df.sample(.2, 10)
    randomsample_manu_data_raw_df.show(5, False)
    '''
    By seeing the some sample records its shows the columns station_id and prodid repeats twice, lets make sure both columns
    have same values as per records to confirm before drop the duplicate columns.
    Lets remove the header first and compare the duplicates column
    we found only one record doesn't has duplicates and that too because of wrong delimiter ";" after the first column instead
    of "," delimiter, these wrong delimiter records will get filter in the malformed data. so we are good to drop the duplicate
    columns. 
    '''
    manu_data_raw_nohdr_df = spark.read.csv(location + "manuf_data.csv", inferSchema=True, header=False, mode='permissive')
    hdr_rec = manu_data_raw_nohdr_df.first()
    manu_data_raw_nohdr_df1 = manu_data_raw_nohdr_df.filter(col("_c0") != hdr_rec[0])
    manu_data_raw_nohdr_df1.show(5, False)
    manu_data_raw_nohdr_df1.select("_c0", "_c1", "_c2", "_c5", "_c6").where("_c1 != _c6").show(5, False)
    manu_data_raw_nohdr_df1.select("_c0", "_c1", "_c2", "_c5", "_c6").where("_c2 != _c5").show(5, False)
    manu_data_drop_dup_col_df = manu_data_raw_nohdr_df1.select("_c0", "_c1", "_c2", "_c3", "_c4", "_c7")
    manu_data_drop_dup_col_df.show(5, False)
    manu_data_drop_dup_col_df.printSchema()
    """
    Inferschema says all the columns as string but the sample data says some columns might not be the string by doing 
    the below process its clear that only because of the mal records the column schema is showing as string, lets check
    the unique columns, by seeing the data pattrn it shows the part id is unique in terms of / combination of prod id,
    station id, sensor and its value with timestamp. so at the end of each prod id set we have one status and its value column
    and those value "ok" falls under value column and making it as string instead of double. hence move the satus detail 
    at the end as a new column and pass its respective value column as a value for the status column.
    
    Lets keep the above findings and read the file as rdd to remove the duplicate column and remove the header
    """
    manu_data_drop_dup_col_df.where("upper(_c2) <> lower(_c2)").show(10)
    manu_data_drop_dup_col_df.where("upper(_c0) <> lower(_c0)").show(10)
    manu_data_drop_dup_col_df.where("upper(_c4) <> lower(_c4)").show(10)
    manu_data_drop_dup_col_df.where("_c7 is null").show()
    '''
    lets read the manu data file again as a rdd to play easily remove the columns and filter the record and then format it into 
    dataframe and convert to schema RDD so that we can apply Data munging cleansing scrubbing standardising and publishing the records as tiny data format
    '''
    print("1. Data Munging")
    print("a. Data Discovery (EDA) - Performing (Data Exploration) exploratory data analysis of the raw data to identify the attributes and patterns.")
    schema1 = StructType([StructField("timestamp", StringType(), False),
                         StructField("prod_id", StringType(), True),
                         StructField("station_id", StringType(), False),
                         StructField("sensor", StringType(), False),
                         StructField("value", StringType(), True),
                         StructField("part_id", StringType(), False)])
    raw_manu_rdd = sc.textFile(location + "manuf_data.csv")
    raw_manu_rdd.take(5)

    manu_rdd1_hdr_removed = raw_manu_rdd.map(lambda x: x.split(",")).map(lambda x: (x[0], x[1], x[2], x[3], x[4], x[7])).filter(lambda x: x[0] != 'timestamp')
    print("b. Formation of DataFrame : Applying schema")
    manu_df1 = manu_rdd1_hdr_removed.toDF(schema1)
    '''
    we could see the columns have none and null values so lets replace all the empty values with null so that we can drop it based on the na option. the fields 
    station_id, prod_id and value are mixed with null hence we could n't apply the actual datatype what those fields belongs. so 
    inorder to cleansing the data lets remove the nulls also cast the entire columns respective datatypes
    '''
    # null check
    manu_df1.where("part_id = ''").show()
    # Data in the prod id column has null, may create challenges in the further data processing

    print("c. Data Preparation (Cleansing & Scrubbing) - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies")
    '''
    part id we have empty spaces, lets prepare the data by replacing all the empty with null so that we can easily cleansing the data
    '''
    manu_df1_filling = manu_df1.select([when(col(cols) == "", None).otherwise(col(cols)).alias(cols) for cols in manu_df1.columns])
    manu_df1_filling.where("part_id = ''").show()
    manu_df1_filling.where("part_id is null").show()
    manu_df1_cleansing = manu_df1_filling.na.drop("any", subset=["part_id"])  # select("_1", "_2", col("_3").cast("int"), "_4", col("_5").cast("float"), "_6").na.drop("any", subset=["_1", "_2", "_3", "_4", "_5", "_6"])
    manu_df1_cleansing.where("part_id is null").show()
    len(manu_df1_cleansing.collect())  # 2518
    manu_df1_cleansing.show(10, False)

    '''
    For each part_id we could see set of sensor range and at the end of each set we have a status column which says value as ok, for 
    other range we are haveing sense value except this status one. So as part of    we can move that status as a separate column and
    populate the respective ok value for the part_id in the new column 
    '''
    print("*************** Data Enrichment and Wrangling (values)-> Add, Rename")
    manu_df1_enrich = manu_df1_cleansing.select("timestamp", "prod_id", "station_id", when(col("sensor") == 'STATUS', col("value")).otherwise(lit("")).alias("status"), when(col("sensor") == 'STATUS', lit("")).otherwise(col("sensor")).alias("sensor"), when(col("value") == 'OK', None).otherwise(col("value")).alias("value"), "part_id")
    manu_df1_enrich.show(10, False)
    print(" Column re-order/number of columns to make it in a usable format final Tidy data")
    manu_df1_final_tidy = manu_df1_enrich.select("timestamp", "prod_id", "station_id", "sensor", "value", "part_id", "status")
    len(manu_df1_final_tidy.collect())
    manu_df1_final_tidy.show(10, False)
    '''
    print("Write the data into HDFS table")
    spark.sql("drop table if exists manu_final_tidy_tbl")
    spark.sql("""
    CREATE EXTERNAL TABLE `manu_final_tidy_tbl`(
    `timestamp` string, 
    `prod_id` string, 
    `station_id` string, 
    `sensor` string, 
    `value` string, 
    `part_id` string, 
    `status` string)
    ROW FORMAT SERDE 
     'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
    WITH SERDEPROPERTIES ( 
    'field.delim'=',', 
    'serialization.format'=',') 
    STORED AS INPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
    OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
    'hdfs://localhost:54310/user/hduser/default/'
        """)
    manu_df1_final_tidy.write.mode("append").format("Hive").saveAsTable("manu_final_tidy_tbl")
    '''
    print("Write the data as a single CSV file")
    manu_df1_final_tidy.coalesce(1).write.mode("overwrite").csv(location + "\\write\\manu_final_tidy_csv", header=True)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import *
        from pyspark.sql.types import *
        main(sys.argv)
    else:
        print("No enough argument to continue running this program")
        exit(1)

