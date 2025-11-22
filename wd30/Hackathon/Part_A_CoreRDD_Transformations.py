from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.storagelevel import StorageLevel
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.master("local[*]").appName("Hackathon_2023").getOrCreate()
sc = spark.sparkContext
location = "E:\\BigData\\Shared_Documents\\sample_data\\sparkhack2\\"


print("1. Load the file1 (insuranceinfo1.csv) from HDFS using textFile API into an RDD insuredata")
insuredata = sc.textFile(location + "insuranceinfo1.csv")
print(insuredata.collect())
print(insuredata.count())
insuredata.take(3)


print("2. Remove the header line from the RDD contains column names.")
print("2.1 If we know the column name then just use filter")
map_rdd = insuredata.map(lambda x:x.split(","))
insuredata_rem_hdr = map_rdd.filter(lambda x: x[0] != 'IssuerId')
print("2.2 If we don't know the column name then use first and filter")
first_hdr_rec = insuredata.first()
insuredata_rem_first = insuredata.filter(lambda x:x != first_hdr_rec)
insuredata_rem_first.count()


print("3. Remove the Footer/trailer also which contains “footer,402,,,,,,,,”")
last_rec_index = insuredata_rem_first.count() - 1
insuredata_rem_hdr_footer = insuredata_rem_first.zipWithIndex().filter(lambda x: x[1] != last_rec_index).map(lambda x: x[0])


print("4. Display the count and show few rows and check whether header and footer is removed.")
insuredata_rem_hdr_footer.count()
insuredata_rem_hdr_footer.take(5)
insuredata_rem_hdr_footer.map(lambda x: x[0]).collect()  # we don't see header and footer 'f' in the x[0] first char


print("5. Remove the blank lines in the rdd and Remove the leading and trailing spaces ")
insuredata_rem_hdr_footer.map(lambda x: x.strip()).take(3)
insuredata_len = insuredata_rem_hdr_footer.map(lambda x: x.strip()).filter(lambda x: len(x) > 0)


print("6. Map and split u")
insuredata_split = insuredata_len.map(lambda x: x.split(",", -1))
insuredata_split.count()
insuredata_split.first()


print("7. Filter number of fields are equal to 10 columns only")
insuredata_final_rdd = insuredata_split.filter(lambda x: len(x) == 10)
'''
My View about this is, If we convert this RDD to DF to apply struct schema/structure to these records, we are not sure 
at which column what value should get populate. Also when we use unionbyname and merging thos scenarios lead to issues,
so in the starting stage itself its better if we filter these kind of malformed records before converting to DF
'''


print("""8. Take the count of the RDD created in step 7 and step 1 and print how many rows are 
removed/rejected in the cleanup process of removing the number of fields does not 
equals 1""")
insuredata_final_cnt = insuredata.count() - insuredata_final_rdd.count()
print(f"We have removed/rejected {insuredata_final_cnt} rows as part of the cleanup process ")


print("9. Create another RDD namely rejectdata and store the row that does not equals 10 with the no of columns it has")
insuredata_deficient_rec = insuredata_split.filter(lambda x: len(x) != 10).map(lambda x: (len(x), x))


print("10. Load the file2 (insuranceinfo2.csv) from HDFS using textFile API into an RDD insuredata2")
insuredata2 = sc.textFile(location + "insuranceinfo2.csv")


print("11. Repeat from step 2 to 8 for this file also")
first_hdr_rec2 = insuredata2.first()
insuredata2_rem_first = insuredata2.filter(lambda x:x != first_hdr_rec2)
insuredata2_rem_first.count()
last_rec_index2 = insuredata2_rem_first.count() - 1
insuredata2_rem_hdr_footer = insuredata2_rem_first.zipWithIndex().filter(lambda x: x[1] != last_rec_index2).map(lambda x: x[0])
insuredata2_rem_hdr_footer.count()
insuredata2_rem_hdr_footer.take(5)
insuredata2_len = insuredata2_rem_hdr_footer.filter(lambda x: len(x) > 0)
insuredata2_split = insuredata2_len.map(lambda x: x.split(",", -1))
insuredata2_split.count()
insuredata2_split.first()
insuredata2_split_filter_columns = insuredata2_split.filter(lambda x: len(x) == 10)
insuredata2_final_cnt = insuredata2.count() - insuredata2_split_filter_columns.count()
print(f"We have removed/rejected {insuredata2_final_cnt} rows as part of the cleanup process ")
insuredata2_deficient_rec = insuredata2_split.filter(lambda x: len(x) != 10).map(lambda x: (len(x), x))


print("""create the schema rdd from the insuranceinfo2.csv and filter the records that contains blank or null IssuerId,IssuerId2""")
first_hdr_rec2 = 'IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,NetworkName,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan'
#schema_insuredata2 = insuredata2_split_filter_columns.map(lambda cols: {"IssuerId": cols[0], "IssuerId2": cols[1], "BusinessDate": cols[2], "StateCode": cols[3], "SourceName": cols[4], "NetworkName": cols[5], "NetworkURL": cols[6], "custnum": cols[7], "MarketCoverage": cols[8], "DentalOnlyPlan": cols[9]})
schema_insuredata2 = insuredata2_split_filter_columns.map(lambda x: Row(IssuerId=x[0], IssuerId2=x[1], BusinessDate=x[2], StateCode=x[3], SourceName=x[4], NetworkName=x[5], NetworkURL=x[6], custnum=x[7], MarketCoverage=x[8], DentalOnlyPlan=x[9]))
schema_insuredata = insuredata_final_rdd.map(lambda x: Row(IssuerId=x[0], IssuerId2=x[1], BusinessDate=x[2], StateCode=x[3], SourceName=x[4], NetworkName=x[5], NetworkURL=x[6], custnum=x[7], MarketCoverage=x[8], DentalOnlyPlan=x[9]))
schema_insuredata2_rem_null = schema_insuredata2.filter(lambda x: (x.IssuerId != '' and x.IssuerId2 != ''))
schema_insuredata2_rem_null.count()
schema_insuredata2_rem_null.map(lambda x: x[0]).collect()


print("2. Data merging, Deduplication, Performance Tuning & Persistance 15% Completion) – Total 35%")
print("12. Merge the both header and footer removed RDDs derived in steps 8 and 11 into an RDD namely insuredatamerged")
insuredatamerged = schema_insuredata.union(schema_insuredata2_rem_null)


print("13. Persist the step 12 RDD to memory by serializing.")
insuredatamerged.persist(StorageLevel.MEMORY_ONLY)


print("14. Calculate the count of rdds created in step 8+11 and rdd in step 12, check whether they are matching.")
total_insure_cnt = schema_insuredata.count() + schema_insuredata2_rem_null.count()
insuredatamerged_cnt = insuredatamerged.count()
if total_insure_cnt == insuredatamerged_cnt:
    print(f"The sum of insuredata1 and insuredata2 count {total_insure_cnt} is matched with merged data count {insuredatamerged_cnt}")
else:
    print("The count is not matched")



print("15. Increase the number of partitions in the above rdd to 8 partitions and name it as insuredatarepart.")
insuredatamerged.getNumPartitions()
insuredatarepart = insuredatamerged.repartition(8)
insuredatarepart.getNumPartitions()


print("16. Split the above RDD using the businessdate field into rdd_20191001 and rdd_20191002 based on the "
      "BusinessDate of 2019-10-01 and 2019-10-02 respectively using Filter function.")
rdd_20191001 = insuredatarepart.filter(lambda x: (x.BusinessDate == '2019-10-01' or x.BusinessDate == '01-10-2019'))
rdd_20191002 = insuredatarepart.filter(lambda x: (x.BusinessDate == '2019-10-02' or x.BusinessDate == '02-10-2019'))
rdd_20191001.count()
rdd_20191002.count()


print("17. Store the RDDs created in step 15, 16 into HDFS location.")
insuredatarepart.saveAsTextFile("hdfs:///user/hduser/insuredatarepart")
rdd_20191001.saveAsTextFile("hdfs:///user/hduser/rdd_20191001")
rdd_20191002.saveAsTextFile("hdfs:///user/hduser/rdd_20191002")


print("18. Convert the RDDs created in step 15 above into Dataframe namely insuredaterepartdf")
insuredaterepartdf = insuredatarepart.toDF().toDF("IssuerId", "IssuerId2","BusinessDate","StateCode","SourceName","NetworkName","NetworkURL","custnum","MarketCoverage","DentalOnlyPlan")
insuredaterepartdf.coalesce(1).write.option("header","True").mode("overwrite").csv(location + "\\write\\insuredaterepart")


