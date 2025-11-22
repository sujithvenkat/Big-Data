print("3. Remove the Footer/trailer also which contains “footer,402,,,,,,,,”")
last_rec_index = insuredata_rem_first.count() - 1
insuredata_rem_first = insuredata_rem_first.zipWithIndex().filter(lambda x: x[1] != last_rec_index).map(lambda x: x[0])
last_rec_str = str(last_rec).replace("[[","").replace("]]","")
insuredata_rem_first.filter(lambda x: x[0] == last_rec_str).collect()
insuredata_rem_first.map(lambda x: x[1] if int(x[0]) == 'footer count is 402' else x[1]).take(2)
insuredata_rem_first.filter(lambda x: x[0] == 'footer count is 402').collect()
insuredata_hdr_foot_rmvd = insuredata_rem_first.filter(lambda x: x[0] == last_rec)
insuredata_hdr_foot_rmvd.count()
last_rec.collect()


#  try to remove records with null values without any comma separated
print("5. Remove the blank lines in the rdd. ")
insuredata_len = insuredata_rem_hdr_footer.filter(lambda x: len(x) > 0)
insuredata_split = insuredata_len.map(lambda x: x.split(",", -1))



#  instead of hardcoding header names try to parametirized
print("""create the schema rdd from the insuranceinfo2.csv and filter the records that contains blank or null IssuerId,IssuerId2""")
first_hdr_rec2 = 'IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,NetworkName,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan'
schema_insuredata2 = insuredata2_split_filter_columns.map(lambda x: Row(IssuerId=x[0], IssuerId2=x[1], BusinessDate=x[2], StateCode=x[3], SourceName=x[4], NetworkName=x[5], NetworkURL=x[6], custnum=x[7], MarketCoverage=x[8], DentalOnlyPlan=x[9]))
schema_insuredata2_rem_null = schema_insuredata2.filter(lambda x: (x[0] and x[1] and x[2] and x[3] and x[4] and x[5] and x[6] and x[7] and x[8] and x[9] != ''))
schema_insuredata2_rem_null.count()
schema_insuredata2_rem_null.map(lambda x: x[0]).collect()


# insrtead of giving diff date formats try to change the date format in file
rdd_20191001 = insuredatarepart.filter(lambda x: (x.BusinessDate == '2019-10-01' or x.BusinessDate == '01-10-2019'))
rdd_20191002 = insuredatarepart.filter(lambda x: (x.BusinessDate == '2019-10-02' or x.BusinessDate == '02-10-2019'))


# apply this in dataframe of rejected records
print(". Ignoreleading and trailing whitespace")