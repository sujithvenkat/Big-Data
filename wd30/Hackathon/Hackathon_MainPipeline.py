import sys
import os
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def core_rdd_transformations_actions(sc, location, hdfs_location):
    print("\n1. Load the file1 (insuranceinfo1.csv) from HDFS using textFile API into an RDD insuredata")
    insuredata = sc.textFile(hdfs_location + "insuranceinfo1.csv")
    print(insuredata.take(2))
    print("The Total records in insuredata1 : ", insuredata.count())


    print("\n2. Remove the header line from the RDD contains column names.")
    print("2.1 If we know the column name then just use filter")
    map_rdd = insuredata.map(lambda x: x.split(","))
    insuredata_rem_hdr = map_rdd.filter(lambda x: x[0] != 'IssuerId')
    print("2.2 If we don't know the column name then use first and filter")
    first_hdr_rec = insuredata.first()
    insuredata_rem_first = insuredata.filter(lambda x: x != first_hdr_rec)
    #print(insuredata_rem_first.take(2))


    print("\n3. Remove the Footer/trailer also which contains “footer,402,,,,,,,,”")
    last_rec_index = insuredata_rem_first.count() - 1
    insuredata_rem_hdr_footer = insuredata_rem_first.zipWithIndex().filter(lambda x: x[1] != last_rec_index).map(lambda x: x[0])


    print("\n4. Display the count and show few rows and check whether header and footer is removed.")
    print("The count after removed header and footer insuredata : ", insuredata_rem_hdr_footer.count())
    print("The sample data without header and footer.. \n", insuredata_rem_hdr_footer.take(5))
    #print(insuredata_rem_hdr_footer.map(lambda x: x[0]).collect())  # we don't see header and footer 'f' in the x[0] first char


    print("\n\nCleanup process started....")
    print("\n5. Remove the blank lines in the rdd and Remove the leading and trailing spaces ")
    insuredata_len = insuredata_rem_hdr_footer.map(lambda x: x.strip()).filter(lambda x: len(x) > 0)


    print("6. Map and split using ‘,’ delimiter. ")
    insuredata_split = insuredata_len.map(lambda x: x.split(",", -1))


    print("7. Filter number of fields are equal to 10 columns only")
    insuredata_final_rdd = insuredata_split.filter(lambda x: len(x) == 10)
    print('''
    My View about this is, Its better if we filter these kind of malformed records in the starting stage itself before converting to DF
    If we convert this RDD to DF to apply struct schema/structure to these records, we are not sure at which column what value will get
    populate because of lack of columns. Also when we use unionbyname and merging those scenarios lead to issues''')


    print("""8. Take the count of the RDD created in step 7 and step 1 and print how many rows are removed/rejected in 
    the cleanup process of removing the number of fields does not equals 10""")
    insuredata_final_cnt = insuredata.count() - insuredata_final_rdd.count()
    print(f"\nAs part of the cleanup process 5 - 8, We have removed/rejected {insuredata_final_cnt} rows")
    print("The sample final data after Cleanup process....\n", insuredata_final_rdd.take(3))


    print("\n9. Create another RDD namely rejectdata and store the row that does not equals 10 with the no of columns it has")
    insuredata_deficient_rec = insuredata_split.filter(lambda x: len(x) != 10).map(lambda x: (len(x), x))
    print("The sample rejected records....\n", insuredata_deficient_rec.take(2))


    print("\n10. Load the file2 (insuranceinfo2.csv) from HDFS using textFile API into an RDD insuredata2")
    insuredata2 = sc.textFile(hdfs_location + "insuranceinfo2.csv")
    print("The Total records in insuredata2 : ", insuredata2.count())


    print("\n11.a. Repeat from step 2 to 8 for this file also")
    first_hdr_rec2 = insuredata2.first()
    insuredata2_rem_first = insuredata2.filter(lambda x: x != first_hdr_rec2)
    last_rec_index2 = insuredata2_rem_first.count() - 1
    insuredata2_rem_hdr_footer = insuredata2_rem_first.zipWithIndex().filter(lambda x: x[1] != last_rec_index2).map(
        lambda x: x[0])
    print("The count after removed header and footer in insuredata2 : ", insuredata2_rem_hdr_footer.count())
    print("The sample data without header and footer.. \n", insuredata2_rem_hdr_footer.take(5))
    print("\nCleanup process started....")
    print("\nRemove blank, split using delimiter, filter in rec only with 10 columns\n")
    insuredata2_len = insuredata2_rem_hdr_footer.filter(lambda x: len(x) > 0)
    insuredata2_split = insuredata2_len.map(lambda x: x.split(",", -1))
    #insuredata2_split.count()
    #insuredata2_split.first()
    insuredata2_split_filter_columns = insuredata2_split.filter(lambda x: len(x) == 10)
    insuredata2_final_cnt = insuredata2.count() - insuredata2_split_filter_columns.count()
    print(f"As part of the cleanup process, We have removed/rejected {insuredata2_final_cnt} rows")
    print("The sample final data after Cleanup process....\n", insuredata2_split_filter_columns.take(3))
    insuredata2_deficient_rec = insuredata2_split.filter(lambda x: len(x) != 10).map(lambda x: (len(x), x))
    print("The sample rejected records....\n", insuredata2_deficient_rec.take(2))


    print("""\n11 b.create the schema rdd from the insuranceinfo2.csv and filter the records that contains blank or null IssuerId,IssuerId2""")
    first_hdr_rec2 = 'IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,NetworkName,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan'
    # schema_insuredata2 = insuredata2_split_filter_columns.map(lambda cols: {"IssuerId": cols[0], "IssuerId2": cols[1], "BusinessDate": cols[2], "StateCode": cols[3], "SourceName": cols[4], "NetworkName": cols[5], "NetworkURL": cols[6], "custnum": cols[7], "MarketCoverage": cols[8], "DentalOnlyPlan": cols[9]})
    schema_insuredata2 = insuredata2_split_filter_columns.map(
        lambda x: Row(IssuerId=x[0], IssuerId2=x[1], BusinessDate=x[2], StateCode=x[3], SourceName=x[4],
                      NetworkName=x[5], NetworkURL=x[6], custnum=x[7], MarketCoverage=x[8], DentalOnlyPlan=x[9]))
    schema_insuredata = insuredata_final_rdd.map(
        lambda x: Row(IssuerId=x[0], IssuerId2=x[1], BusinessDate=x[2], StateCode=x[3], SourceName=x[4],
                      NetworkName=x[5], NetworkURL=x[6], custnum=x[7], MarketCoverage=x[8], DentalOnlyPlan=x[9]))
    schema_insuredata2_rem_null = schema_insuredata2.filter(lambda x: (x.IssuerId != '' and x.IssuerId2 != ''))
    print("The count after removed the nulls or blanks in IssuerIds : " , len(schema_insuredata2_rem_null.map(lambda x: x[0]).collect()))
    print("The Final sample records after applied schema and filtered null...\n", schema_insuredata2_rem_null.take(2))
    #schema_insuredata2_rem_null.map(lambda x: x[0]).collect()


    print("\n\n2. Data merging, Deduplication, Performance Tuning & Persistance 15% Completion) – Total 35%")
    print("\n12. Merge the both header and footer removed RDDs derived in steps 8 and 11 into an RDD namely insuredatamerged")
    insuredatamerged = schema_insuredata.union(schema_insuredata2_rem_null)
    print("The sample merged recs... \n", insuredatamerged.take(2))


    print("\n13. Persist the step 12 RDD to memory by serializing.")
    insuredatamerged.persist(StorageLevel.MEMORY_ONLY)
    print("The merged insuredata persisted into memory")


    print("\n14. Calculate the count of rdds created in step 8+11 and rdd in step 12, check whether they are matching.")
    total_insure_cnt = schema_insuredata.count() + schema_insuredata2_rem_null.count()
    insuredatamerged_cnt = insuredatamerged.count()
    if total_insure_cnt == insuredatamerged_cnt:
        print( f"The sum of insuredata1 and insuredata2 count {total_insure_cnt} is matched with merged data count {insuredatamerged_cnt}")
    else:
        print("The count is not matched")


    print("\n15. Increase the number of partitions in the above rdd to 8 partitions and name it as insuredatarepart.")
    insuredatamerged.getNumPartitions()
    insuredatarepart = insuredatamerged.repartition(8)
    print("The Number of partition now is : ", insuredatarepart.getNumPartitions())


    print("\n16. Split the above RDD using the businessdate field into rdd_20191001 and rdd_20191002 based on the "
          "BusinessDate of 2019-10-01 and 2019-10-02 respectively using Filter function.")
    rdd_20191001 = insuredatarepart.filter(lambda x: (x.BusinessDate == '2019-10-01' or x.BusinessDate == '01-10-2019'))
    rdd_20191002 = insuredatarepart.filter(lambda x: (x.BusinessDate == '2019-10-02' or x.BusinessDate == '02-10-2019'))
    print("Rec count in rdd_20191001 : ", rdd_20191001.count())
    print("Rec count in rdd_20191002 : ", rdd_20191002.count())


    print("\n17. Store the RDDs created in step 15, 16 into HDFS location.")
    insuredatarepart.saveAsTextFile(hdfs_location + "\\write\\insuredatarepart")
    rdd_20191001.saveAsTextFile(hdfs_location + "\\write\\rdd_20191001")
    rdd_20191002.saveAsTextFile(hdfs_location + "\\write\\rdd_20191002")
    print("The Date splitted records are saved into respective hdfs location")


    print("\n18. Convert the RDDs created in step 15 above into Dataframe namely insuredaterepartdf")
    insuredaterepartdf = insuredatarepart.toDF().toDF("IssuerId", "IssuerId2", "BusinessDate", "StateCode",
                                                      "SourceName", "NetworkName", "NetworkURL", "custnum",
                                                      "MarketCoverage", "DentalOnlyPlan")
    print("The Final RDD to Dataframe sample rows..  \n ")
    insuredaterepartdf.show(2, False)
    insuredaterepartdf.coalesce(1).write.option("header", "True").mode("overwrite").csv(
        location + "\\write\\insuredaterepart")


def write_to_file(df, filetype, hdfs_location, delimiter, md):
    print("The function write_to_file got created to write all type of files")
    if filetype == 'csv':
        df.write.mode(md).csv(hdfs_location, header=True, sep=delimiter)
    elif filetype == 'json':
        df.write.mode(md).json(hdfs_location)
    print(f"The Insuredata written into {hdfs_location} via the function write_to_file in the {filetype} format ")


def spark_df_dsl_sql(spark, sc, location, hdfs_location):
    print("\n19. Dataframe creation using the built in modules")
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


    print("""\n20.A Create dataframe using the csv module accessing the insuranceinfo1.csv file and remove the footer from both using header true and remove the footer using dropmalformed 
    options and apply the schema of the structure type created in the step 19.A""")
    insuredata_df = spark.read.csv(hdfs_location + "insuranceinfo1.csv", mode='dropmalformed', schema=structuretypes1,header=True)
    print("\n The insuredata_df got created with the Schema.. Sample rows")
    insuredata_df.show(3)
    print("The total records count : ", len(insuredata_df.collect()))

    print("""\n20.B Create another dataframe using the csv accessing the insuranceinfo2.csv file and remove the header from both dataframes using header true and remove the footer 
    using dropmalformed options and apply the schema of the structure type created in the step 19.B""")
    insuredata2_df = spark.read.csv(hdfs_location + "insuranceinfo2.csv", mode='dropmalformed', schema=structuretypes2,header=True)
    print("""As the BusinessDate column format is dd-MM-yyyy, we need to explicitly convert it to yyyy-MM-dd formatn""")
    insuredata2_df = insuredata2_df.withColumn("BusinessDate", to_date("BusinessDate", "dd-MM-yyyy"))
    print("\n The insuredata2_df got created with the Schema.. Sample rows")
    insuredata2_df.show(3)
    print("The total records count : ", len(insuredata2_df.collect()))

    print("""\n20.C Create another dataframe using the csv accessing the insuranceinfo2.csv file and remove the header from the dataframe
    using header true, permissive options and apply the schema of the structure type created in the step 19.C""")
    rejected_df = spark.read.csv(hdfs_location + "insuranceinfo2.csv", mode='permissive', schema=structuretypes3, header=True, columnNameOfCorruptRecord='RejectRows')
    rejected_df.cache()
    print("\n The rejected_df got created with the Schema.. Sample RejectRows")
    rejected_df.select("RejectRows").where("RejectRows is not null").show()
    print("II. Ignoreleading and trailing whitespaces")
    for rej_col_name in rejected_df.columns:
        rejected_df.withColumn(rej_col_name, trim(col(rej_col_name)))
    rejected_df.coalesce(1).write.mode("overwrite").csv(location + "\\write\\rejectedrows_df")
    print(f"The rejected_df written into {location}")

    print("\n\n21. Apply the below DSL functions in the DFs created in step 20")
    insuredata_merged = insuredata_df.union(insuredata2_df)
    print("21.a. Rename the fields StateCode and SourceName as stcd and srcnm respectively.")
    insuredata_col_rename_df1 = insuredata_merged.withColumnRenamed("StateCode", "stcd").withColumnRenamed("SourceName","srcnm")
    print("The Sample records after applied DSL rename functions...\n")
    insuredata_col_rename_df1.show(2)

    print("21.b. Concat IssuerId,IssuerId2 as issueridcomposite and make it as a new field ")
    insuredata_concat_df2 = insuredata_col_rename_df1.withColumn("issueridcomposite",concat(col("IssuerId").cast("string"), col("IssuerId2").cast("string")))
    print("The Sample records after applied DSL concat functions...\n")
    insuredata_concat_df2.show(2)

    print("21.C. Remove DentalOnlyPlan column")
    insuredata_drop_col_df3 = insuredata_concat_df2.drop("DentalOnlyPlan")
    print("The Sample records after applied DSL drop functions...\n")
    insuredata_drop_col_df3.show(2)

    print("21.d. Add columns that should show the current system date and timestamp with the fields name of sysdt and systs respectively.")
    insuredata_add_sysdt_time_df4 = insuredata_drop_col_df3.withColumn("sysdt", current_date()).withColumn("systm",current_timestamp())
    print("The Sample records after applied DSL add col functions...\n")
    insuredata_add_sysdt_time_df4.show(2, False)

    print("\nSeparate usecases")
    print("i. Identify all the column names and store in an List variable – use columns function.")
    col_list = insuredata_add_sysdt_time_df4.columns
    print("The col_list is : ", col_list)

    print("ii. Identify all columns with datatype and store in a list variable and print use dtypes function")
    col_list_dtypes = insuredata_add_sysdt_time_df4.dtypes
    print("The col_list with datatype is : ", col_list_dtypes)

    print("iii. Identify all integer columns alone and store in an list variable and print.")
    col_dict_dtypes = dict(col_list_dtypes)
    col_int_types = []
    for col_name in col_dict_dtypes:
        if col_dict_dtypes[col_name] == 'int':
            col_int_types.append(col_name)
    print(f"The integer columns list is : {col_int_types}")

    print("iv. Select only the integer columns identified in the above statement and show 10 records in the screen")
    insuredata_add_sysdt_time_df4.select(col_int_types).show(5)

    print("v. Identify the additional column in the reject dataframe created in step 20 above by subtracting the "
          "columns between dataframe1 and dataframe3 created in step20.")
    insuredata_df1_col_list = insuredata_df.columns
    reject_col_list = rejected_df.columns
    insur_cols = set(insuredata_df1_col_list)
    reject_rows_cols = set(reject_col_list)
    additional_cols = reject_rows_cols - insur_cols
    print(f"The additional column is {additional_cols}")

    print("""\n22. Take the DF created in step 21.d and Remove the rows contains null in any one of the 
    field and count the number of rows which contains all columns with some value. """)
    insuredata_drop_null_df5 = insuredata_add_sysdt_time_df4.na.drop('any')
    print("The Sample rows after dropped the rows with even any one column has null")
    insuredata_drop_null_df5.show(2, False)
    print("Total Cleaned records count : ", len(insuredata_drop_null_df5.collect()))

    print("\n23. Custom Method creation:")
    remspecialchar_UDF = udf(remspecialchar)
    print("Registered UDF for DSL")

    print("\n25. Call the above udf in the DSL by passing NetworkName column as an argument to get the special characters removed DF.")
    insuredata_udf_df6 = insuredata_drop_null_df5.withColumn("NetworkName", remspecialchar_UDF("NetworkName"))
    print("The Sample rows in NetworkName column before apply UDF")
    insuredata_drop_null_df5.select("NetworkName").show(30, False)
    print("The Sample rows in NetworkName column after applied UDF")
    insuredata_udf_df6.select("NetworkName").show(30, False)

    print("\n26. Save the DF generated in step 25 in JSON format into HDFS with overwrite option.")
    insuredata_udf_df6.write.mode("overwrite").json(hdfs_location + "\\write\\insuredata_udf_json")
    print("The records written into hdfs location as JSON format")

    print("\n27. Save the DF generated in step 25 into CSV format with header name as per the DF and "
          "delimited by ~ into HDFS with overwrite option ")
    insuredata_udf_df6.write.mode("overwrite").csv(hdfs_location + "\\write\\insuredata_udf_csv", header=True, sep="~")
    print("The records written into hdfs location as CSV format")

    print("\n27.b. Note: Create a generic function namely writeToFile  to save the data rather than calling the write.csv "
          "and write.json directly.")
    write_to_file(insuredata_udf_df6, 'csv', hdfs_location + "\\write\\insuredata_udf_csv", "~", "overwrite")
    write_to_file(insuredata_udf_df6, 'json', hdfs_location + "\\write\\insuredata_udf_json", "", "overwrite")

    print("""\n28. Save the DF generated in step 25 into hive external table and append the data without 
    overwriting it""")
    '''spark.sql("drop table if exists insuredata_final")
    spark.sql("CREATE EXTERNAL TABLE `insuredata_final`(`issuerid` int, `issuerid2` int, `businessdate` date, `stcd` string, `srcnm` string, `networkname` string, `networkurl` string, `custnum` string, `marketcoverage` string, `issueridcomposite` string, `sysdt` date,`systm` timestamp) row format delimited fields terminated by ',' stored as Parquet LOCATION 'hdfs://localhost:54310/user/hduser/default'""")
    insuredata_udf_df6.write.mode("append").format("Hive").saveAsTable("insuredata_final")'''

    print("\n\n4. Tale of handling RDDs, DFs and TempViews (20% Completion) – Total 75%")
    print("Loading RDDs, split RDDs, Load DFs, Split DFs, Load Views, Split Views, write UDF, register to use in Spark SQL, "
          "Transform, Aggregate, store in disk/DB")

    print("\nUse RDD functions:")
    print("""\n29. Load the file3 (custs_states.csv) from the HDFS location, using textfile API in an RDD custstates, 
    this file contains 2 type of data one with 5 columns contains customer master info and other data with statecode and  description of 2 columns.""")
    custstates = sc.textFile(hdfs_location + "custs_states.csv")
    print("The Total records in custstates : ", custstates.count())

    print("""\n30. Split the above data into 2 RDDs, first RDD namely custfilter should be loaded only with 5 columns data 
    and second RDD namely statesfilter should be only loaded with 2 columns data.""")
    custstates_split = custstates.map(lambda x: x.split(",")).map(lambda x: (x, len(x)))
    print("Persist the custstates_split into cache memory..")
    custstates_split.cache()
    custfilter = custstates_split.filter(lambda x: x[1] == 5).map(lambda x: x[0])
    statesfilter = custstates_split.filter(lambda x: x[1] == 2).map(lambda x: x[0])
    print("The sample records in RDD after filtered by custs..\n", custfilter.take(5))
    print(f"custfilter has {custfilter.count()} records\n")  # 810
    print("The sample records in RDD after filtered by states..\n", statesfilter.take(5))
    print(f"statefilter has {statesfilter.count()} records\n") # 51

    print("\nUse DSL functions:")
    print("""\n31. Load the file3 (custs_states.csv) from the HDFS location, using CSV Module in a DF custstatesdf, this file
    contains 2 type of data one with 5 columns contains customer master info and other data with statecode and description 
    of 2 columns.""")
    custstatesdf = spark.read.csv(hdfs_location + "custs_states.csv", header=False)
    print("The Total records in custstatesdf : ", custstatesdf.count())

    print("""\n32. Split the above data into 2 DFs, first DF namely custfilterdf should be loaded only with 5 columns data 
    and second DF namely statesfilterdf should be only loaded with 2 columns data""")
    custfilterdf_pre = custstatesdf.where("_c3 is not null or _c4 is not null")
    custfilterdf = custfilterdf_pre.select(col("_c0").cast("int").alias("custid"), col("_c1").alias("custfname"),
                                           col("_c2").alias("custlname"),
                                           col("_c3").cast("int").alias("custage"), col("_c4").alias("custprofession"))
    custfilterdf.printSchema()
    print("The sample records in DF after filtered by cust...\n")
    custfilterdf.show(5)
    print(f"custfilterdf has {custfilterdf.count()} records\n")
    statesfilterdf_pre = custstatesdf.filter("_c3 is null and _c4 is null")
    statesfilterdf = statesfilterdf_pre.select(col("_c0").alias("statecode"), col("_c1").alias("statename"))
    statesfilterdf.printSchema()
    print("The sample records in DF after filtered by states...\n")
    statesfilterdf.show(5)
    print(f"statesfilterdf has {statesfilterdf.count()} records\n")

    print("\nUse SQL Queries:")

    print("\n33. Register the above step 32 two DFs as temporary views as custview and statesview.")
    custfilterdf.createOrReplaceTempView("custview")
    statesfilterdf.createOrReplaceTempView("statesview")
    insuredata_add_sysdt_time_df4.createOrReplaceTempView(("insureview"))
    spark.udf.register("remspecialchar", remspecialchar)
    print("Views custview, statesview and insureview are created and the udf remspecialchar got registered")

    print("\n36. Write a SQL query with the below processing – set the spark.sql.shuffle.partitions to 4")
    spark.conf.set("spark.sql.shuffle.partitions", 4)
    print("36.a. Pass NetworkName to remspecialcharudf and get the new column called cleannetworkname")
    print("36.b. Add current date, current timestamp fields as curdt and curts.")
    print("36.c  Extract the year and month from the businessdate field and get it as 2 new fields called yr,mth respectively.")
    print("36.d. Extract from the protocol either http/https from the NetworkURL column, if http then print http non secured"
        " if https then secured else no protocol found then display noprotocol.")
    # since we are checking https  in the first when condition, all the https will get passed in the first when itself only the failures will move to second when so http will get cover in the second when
    insuredata_udf_dt_time_yr_mth_protocol_sql = spark.sql(
        "select IssuerId, IssuerId2, BusinessDate, stcd, srcnm, remspecialchar(NetworkName) cleannetworkname, "
        "NetworkURL, custnum, MarketCoverage, issueridcomposite, current_date() curdt, current_timestamp() curts, "
        "year(BusinessDate) yr, month(BusinessDate) mth, case when NetworkURL like 'https%' then 'http secured' "
        "when NetworkURL like 'http%' then 'http non secured' else 'no protocol' end as protocol from insureview")
    insuredata_udf_dt_time_yr_mth_protocol_sql.createOrReplaceTempView("insureview_detailed")
    print("\n The sample records from the view insureview_detailed after applied all the SQLs in step36a-d...")
    spark.sql("select * from insureview_detailed").show(5, False)

    print("""\n36.e. Display all the columns from insureview including the columns derived from above a, b, c, d steps with statedesc column 
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
    insuredata_join_sql.createOrReplaceTempView("Enriched_insureview")
    print("\n The sample records from the view Enriched_insureview after applied join with other views in step36d...")
    spark.sql("select * from insureview_detailed").show(5, False)

    print("\n37. Store the above selected Dataframe in Parquet formats in a HDFS location as a single file.")
    insuredata_join_sql.coalesce(1).write.mode("overwrite").parquet(hdfs_location + "\\write\\insuredata_sql_pq")
    print("The insureview_detailed view got written into the hdfs location")

    print("""\n38. Write an SQL query to identify average age, count group by statedesc, protocol, 
    profession including a seqno column added which should have running sequence 
    number partitioned based on protocol and ordered based on count descending and 
    display the profession whose second highest count of a given state and protocol.
    """)
    insuredata_aggregated = spark.sql("""select * from (select row_number() over(partition by statename, protocol order by count desc) Seqno, Avgage, count, statename, protocol, custprofession from 
    (select statename, protocol, custprofession, avg(custage) as Avgage, count(*) as count from Enriched_insureview where custprofession is not null group by statename, protocol, custprofession) temp) temp1 where Seqno = 2""")
    print("The sample final DF recs with aggregated records..")
    insuredata_aggregated.show(5)
    insuredata_aggregated.coalesce(1).write.mode("overwrite").parquet(hdfs_location + "\\write\\insuredata_sql_pq")

    print("\n39. Store the DF generated in step 38 into MYSQL table insureaggregated.")
    insuredata_aggregated.write.jdbc(url="jdbc:mysql://127.0.0.1:3306/custdb?user=root&password=Root123$",
                                     table="insureaggregated", mode="overwrite",
                                     properties={"driver": 'com.mysql.jdbc.Driver'})
    print("The Final Df written into MSQl with the help of JDBC")


def main(arg):
    print("Hackathon Main Program starts here")
    spark = SparkSession.builder \
        .appName("Hackathon_2023_Application") \
        .enableHiveSupport() \
        .getOrCreate()
    #.config("spark.jars", "/home/hduser/install/mysql-connector-java.jar") \
    sc = spark.sparkContext

    print("Set the logger level to error")
    sc.setLogLevel("ERROR")

    print("Get the base location as parameter (to make the code to run in windows/VM) \n")
    location = arg[1]
    hdfs_location = arg[2]
    print("Here base location is : " + location)
    print("Here hdfs location is : " + hdfs_location)

    print("\nPart A Core RDD Transformation")
    core_rdd_transformations_actions(sc, location, hdfs_location)

    print("\nPart B Spark DSL and SQL")
    spark_df_dsl_sql(spark, sc, location, hdfs_location)


if __name__ == "__main__":
    if len(sys.argv) == 3:
        from pyspark.sql import SparkSession
        from pyspark.sql.types import Row
        from pyspark.storagelevel import StorageLevel
        from pyspark.sql.types import StructField, StringType, StructType, IntegerType, DateType
        from pyspark.sql.functions import to_date, concat, col, current_date, current_timestamp, trim
        from pyspark.sql.functions import udf
        from org.inceptez.hack.allmethods import remspecialchar
        main(sys.argv)
    else:
        print("No enough argument to continue running this program")
        exit(1)
