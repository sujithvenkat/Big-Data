print("Spark core workouts using python. When we invoke spark libraries using python code then it is pyspark")

from pyspark.sql.session import SparkSession
#  /usr/local/spark/python/ pyspark/sql/session.py
#In the python script we have one class SparkSession based on this case we can identify it as a class inti cap for each word
spark = SparkSession.builder.master("local").appName("pyspark pgm").getOrCreate()
sc = spark.sparkContext
#sc.setLogLevel("ERRO
file_rdd1=sc.textFile("file:///home/hduser/sparkdata/empdata.txt")
#print("The given file as it is",file_rdd1.collect())
'''
hdfs_rdd1=sc.textFile("hdfs:///user/hduser/empdata.txt")
print(hdfs_rdd1.collect())
'''

split_rdd1=file_rdd1.map(lambda x:x.split(","))
#print("The splitted file",split_rdd1.collect())

split_rdd1.cache()

filter_rdd1 = split_rdd1.filter(lambda x:x[1] == 'chennai')
#print("Filter without CAPS",filter_rdd1.collect()) # only filter/bring chennai in lower case
filter_rdd1 = split_rdd1.filter(lambda x:x[1].lower() == 'chennai')
#print("Filter even CAPS",filter_rdd1.collect()) # will filter/bring all the chennai both lower & upper

map_rdd1 = split_rdd1.map(lambda x:(x[0],x[3]))
#print("Just print first and Fourth cols ",map_rdd1.collect())
'''
print("check for exception")
try:
        print("In try block")
        map_rdd1 = split_rdd1.map(lambda x:(x[0],x[4]))
# all the records should have 5 column, in this file the last record doesn't have 5th column. so we are getting index out of
#range issue
        print(map_rdd1.collect())
except Exception as err_msg:
        print(f" I am getting printed : IndexError: list index out of range {err_msg}")

#check_exception()

'''
list_num=range(1,21)
list_rdd = sc.range(1,21)
#print(*list_num)  # To print range in python since its a list
#print(list_rdd.collect()) # To print range in pyspark since its a RDD
list1=[1,2,3,4,5,6]
list_rdd_parall= sc.parallelize([1,2,3,4,5,6])
list_rdd_parall= sc.parallelize(list1)
#print(list_rdd_parall.collect())

salary_lst=[2000,40000,2300,45000]
bonus=5000

salary_lst_rdd=sc.parallelize(salary_lst)
salary_lst_rdd_bonus=salary_lst_rdd.map(lambda x:x + bonus)
#print(salary_lst_rdd_bonus.collect())
#####              or              #######
print(list(map(lambda x:x + bonus,salary_lst)))

'''
file_rdd1=sc.textFile("file:///home/hduser/sparkdata/empdata.txt")
filter_direct_rdd=file_rdd1.filter(lambda i:'chennai' in i.lower())
select_dt_amt_rdd=filter_direct_rdd.map(lambda i:(i.split(",")[3],int(i.split(",")[4])))
print(select_dt_amt_rdd.collect())
'''

rows_str=sc.textFile("file:///home/hduser/sparkdata/test_word_count.log")
#print(rows_str.collect())

'''
['----------------------------------------------------------------',
 'Sat Mar 28 00:14:15 IST 2020:',
 'Booting Derby version The Apache Software Foundation - Apache Derby - 10.12.1.1 - (1704137): instance a816c00e-0171-1d4f-173e-0000153ac928 ',
 'on database directory /home/hduser/sparkdata/metastore_db with class loader org.apache.spark.sql.hive.client.IsolatedClientLoader$$anon$1@7286234f ',
 'Loaded from file:/usr/local/spark/jars/derby-10.12.1.1.jar',
 'java.vendor=Oracle Corporation',
 'java.runtime.version=1.8.0_71-b15',
 'user.dir=/home/hduser/sparkdata',
 'os.name=Linux',
 'os.arch=amd64',
 'os.version=2.6.32-642.11.1.el6.x86_64',
 'derby.system.home=null',
 "Database Class Loader started - derby.database.classpath=''"]
'''
rows_str_split = rows_str.map(lambda x:x.split(","))
rows_str_split_flatmap = rows_str_split.flatMap(lambda x:(x,1))

rows_str_course=sc.textFile("file:///home/hduser/sparkdata/course.log")
#rows_str_course.collect()
rows_str_course_lst=rows_str_course.map(lambda x:x.split(","))
#print(rows_str_course_lst.collect())
rows_str_course.map(lambda x:(x,1))
'''
[('hadoop spark hadoop spark kafka datascience', 1), ('spark hadoop spark datascience', 1), ('informatica java aws gcp', 1), ('gcp aws azure spark', 1), ('gcp pyspark hadoop hadoop', 1)]
'''
rows_str_course.flatMap(lambda x:(x,1))
'''
['hadoop spark hadoop spark kafka datascience', 1, 'spark hadoop spark datascience', 1, 'informatica java aws gcp', 1, 'gcp aws azure spark', 1, 'gcp pyspark hadoop hadoop', 1]
'''
# for i in rows_str_course_lst: -- not working
'''for i in [['hadoop spark hadoop spark kafka datascience'], ['spark hadoop spark datascience'], ['informatica java aws gcp'], ['gcp aws azure spark'], ['gcp pyspark hadoop hadoop']]:
    for j in i:
        #print(j.split(" "),end="\n")
        j_split_lst = j.split(" ")
        for k in j_split_lst:
            print(k,1)'''

first_mp= rows_str_course.map(lambda x:x.split(","))
'''
[['hadoop spark hadoop spark kafka datascience'], ['spark hadoop spark datascience'], ['informatica java aws gcp'], ['gcp aws azure spark'], ['gcp pyspark hadoop hadoop']] 
'''
flat_mp=first_mp.flatMap(lambda x:x[0].split(" "))
flat_mp.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
'''
[('hadoop', 5), ('java', 1), ('aws', 2), ('azure', 1), ('spark', 5), ('kafka', 1), ('datascience', 2), ('informatica', 1), ('gcp', 3), ('pyspark', 1)]
'''
flat_mp.map(lambda x:(x,1)).countByKey()
'''
defaultdict(<class 'int'>, {'hadoop': 5, 'spark': 5, 'kafka': 1, 'datascience': 2, 'informatica': 1, 'java': 1, 'aws': 2, 'gcp': 3, 'azure': 1, 'pyspark': 1})
'''
first_mp.map(lambda x:x[0].split(" ")).map(lambda x:((x[0],1),(x[1],1),(x[2],1),(x[3],1)))
'''
[(('hadoop', 1), ('spark', 1), ('hadoop', 1), ('spark', 1)), (('spark', 1), ('hadoop', 1), ('spark', 1), ('datascience', 1)), (('informatica', 1), ('java', 1), ('aws', 1), ('gcp', 1)), (('gcp', 1), ('aws', 1), ('azure', 1), ('spark', 1)), (('gcp', 1), ('pyspark', 1), ('hadoop', 1), ('hadoop', 1))]
'''
rows_str_course.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)

rows_str_course1=sc.textFile("file:///home/hduser/sparkdata/course1.log")
#rows_str_course1.union(rows_str_course).count()  ## with duplicates 10
#rows_str_course1.union(rows_str_course).distinct().count() ## with out duplicates 5

# zip - passive transformation -
rows_str_stud1=sc.textFile("file:///home/hduser/sparkdata/stud_enquiry.csv")
# 1, Anand
# 2, Harish
# 3, Vaishali

rows_str_city=sc.textFile("file:///home/hduser/sparkdata/stud_city.csv")
# chennai
# mumbai
# hyderabad
rows_str_stud1.zip(rows_str_city)
'''
[('1,Anand', 'Chennai'), ('2,Harish', 'Mumbai'), ('3,Vaishali', 'Hyderabad')]
'''
rows_with_header=sc.textFile("file:///home/hduser/sparkdata/records_with_header")
'''
cid,city,device,amt
1,Chennai,Mobile,1000
2,Chennai,Laptop,3000
3,Hyd,mobile,4000
4,Chennai,mobile,2000
5,Mumbai,Laptop,4000
6,Mumbai,Mobile,1500
7,Kolkata,Laptop,5000
8,Chennai,Headset,9000
9,Chennai,Laptop,22222
'''

rows_with_header.zipWithIndex()
'''
[('cid,city,device,amt', 0), ('1,Chennai,Mobile,1000', 1), ('2,Chennai,Laptop,3000', 2), ('3,Hyd,mobile,4000', 3), ('4,Chennai,mobile,2000', 4), ('5,Mumbai,Laptop,4000', 5), ('6,Mumbai,Mobile,1500', 6), ('7,Kolkata,Laptop,5000', 7), ('8,Chennai,Headset,9000', 8), ('9,Chennai,Laptop,22222', 9)]
'''
rows_with_header.zipWithIndex().filter(lambda x:x[1] > 0)
'''
[('1,Chennai,Mobile,1000', 1), ('2,Chennai,Laptop,3000', 2), ('3,Hyd,mobile,4000', 3), ('4,Chennai,mobile,2000', 4), ('5,Mumbai,Laptop,4000', 5), ('6,Mumbai,Mobile,1500', 6), ('7,Kolkata,Laptop,5000', 7), ('8,Chennai,Headset,9000', 8), ('9,Chennai,Laptop,22222', 9)]
'''
rows_skip_header=rows_with_header.zipWithIndex().filter(lambda x:x[1] > 0).map(lambda x:x[0])
'''
['1,Chennai,Mobile,1000', '2,Chennai,Laptop,3000', '3,Hyd,mobile,4000', '4,Chennai,mobile,2000', '5,Mumbai,Laptop,4000', '6,Mumbai,Mobile,1500', '7,Kolkata,Laptop,5000', '8,Chennai,Headset,9000', '9,Chennai,Laptop,22222']
'''
rows_skip_header.map(lambda x:x[3])
'''
['h', 'h', 'y', 'h', 'u', 'u', 'o', 'h', 'h']
'''
rows_skip_header.map(lambda x:x.split(","))
'''
[['1', 'Chennai', 'Mobile', '1000'], ['2', 'Chennai', 'Laptop', '3000'], ['3', 'Hyd', 'mobile', '4000'], ['4', 'Chennai', 'mobile', '2000'], ['5', 'Mumbai', 'Laptop', '4000'], ['6', 'Mumbai', 'Mobile', '1500'], ['7', 'Kolkata', 'Laptop', '5000'], ['8', 'Chennai', 'Headset', '9000'], ['9', 'Chennai', 'Laptop', '22222']]
'''
rows_city_distinct = rows_skip_header.map(lambda x:x.split(",")).map(lambda x:x[1]).distinct()
#rows_skip_header.map(lambda x:x.split(",")).map(lambda x:x[3])
#print(rows_city_distinct.collect())

print("Actions in Spark")

rdd1 = sc.textFile("hdfs:///user/hduser/empdata.txt")
rdd1.collect()
#['ArunKumar,chennai,33,2016-09-20,100000', 'Lara,chennai,55,2016-09-21,10000', 'vasudevan,banglore,43,2016-09-23,90000', 'irfan,chennai,33,2019-02-20,20000', 'basith,CHENNAI,29,2019-04-22']
len(rdd1.collect())
rdd1.map(lambda x:x[0]).collect()
#['A', 'L', 'v', 'i', 'b']

rdd1.map(lambda x:x).collect()
rdd1.map(lambda x:x.split(",")).collect()
'''[
['ArunKumar', 'chennai', '33', '2016-09-20', '100000'], 
['Lara', 'chennai', '55', '2016-09-21', '10000'], 
['vasudevan', 'banglore', '43', '2016-09-23', '90000'], 
['irfan', 'chennai', '33', '2019-02-20', '20000'], 
['basith', 'CHENNAI', '29', '2019-04-22']
]'''

rdd1.map(lambda x:x.split(",")).map(lambda x:x[0]).map(lambda x:x[0]).collect()

#to delete the records
rdd1.zipWithIndex().collect()
#[('ArunKumar,chennai,33,2016-09-20,100000', 0), ('Lara,chennai,55,2016-09-21,10000', 1), ('vasudevan,banglore,43,2016-09-23,90000', 2), ('irfan,chennai,33,2019-02-20,20000', 3), ('basith,CHENNAI,29,2019-04-22', 4)]
rdd1.zipWithIndex().filter(lambda x:x[1] == 0).map(lambda x:x[0]).collect()
#['ArunKumar,chennai,33,2016-09-20,100000']
rdd1.first() # action
#['ArunKumar,chennai,33,2016-09-20,100000']
rdd1.take(1)
#['ArunKumar,chennai,33,2016-09-20,100000']

#to remove basith records since it has no salary column
#rdd2=rdd1.zipWithIndex().filter(lambda x:x[1] == 0).map(lambda x:x[0]).collect()
rdd2 = rdd1.map(lambda x:x.split(",")).filter(lambda x:x[0] != "basith")
#to add bonus 10000 only to salary chennai people
bonus = 10000
rdd2.map(lambda x: (x[1], int(x[4]))).map(lambda x: (x[0], x[1] + bonus) if x[0] == "chennai" else (x[0], x[1])).collect()
#[('chennai', 110000), ('chennai', 20000), ('banglore', 90000), ('chennai', 30000)]
#to take chennai over salary
rdd2.map(lambda x: (x[1], int(x[4]))).reduceByKey(lambda x, y: x+y).collect()
#[('banglore', 90000), ('chennai', 130000)]
rdd2.map(lambda x: (x[1], int(x[4]))).reduceByKey(lambda x, y: x+y).count()
rdd2.map(lambda x: (x[1], int(x[4]))).reduceByKey(lambda x, y: x+y).map(lambda x: x[1]).collect() #[90000, 130000]
rdd2.map(lambda x: (x[1], int(x[4]))).reduceByKey(lambda x, y: x+y).map(lambda x: x[1]).count() #2
rdd2.map(lambda x: (x[1], int(x[4]))).reduceByKey(lambda x, y: x+y).map(lambda x: x[1]).sum() #220000
rdd2.map(lambda x: (x[1], int(x[4]))).reduceByKey(lambda x, y: x+y).map(lambda x: x[1]).max() #130000
rdd2.map(lambda x: (x[1], int(x[4]))).reduceByKey(lambda x, y: x+y).map(lambda x: x[1]).min() #90000
rdd2.map(lambda x: (x[1], int(x[4]))).reduceByKey(lambda x, y: x+y).map(lambda x: x[1]).first() #90000 return a value
rdd2.map(lambda x: (x[1], int(x[4]))).reduceByKey(lambda x, y: x+y).map(lambda x: x[1]).take(1) #[90000] return a list
rdd2.map(lambda x: (x[1], int(x[4]))).reduceByKey(lambda x, y: x+y).map(lambda x: x[1]).top(1) #[130000]  return sorted one

hrdd1 = sc.textFile("file:///home/hduser/sparkdata/header.txt")
hrdd1.count()
hrdd1.collect()
#it has header records, with the help of actions first/take/top we can remove header
#['id,name,age,sal', '1,irfan,37,100000', '2,arun,22,10000', '3,karthik,33,50000']

header = hrdd1.first()
hrdd1.filter(lambda x: x != header).collect()

#lookup
key_val_rdd = rdd2.map(lambda x: (x[1], int(x[4]))).reduceByKey(lambda x, y: x+y)
key_val_rdd.cache()
max_sal = key_val_rdd.map(lambda x: x[1]).max() #130000
key_val_rdd.lookup(max_sal)
#[] because here key is city_name and value is salary, but in lookup we are searching with salary as a key in the rdd

#try to interchange it
key_val_rdd = rdd2.map(lambda x: (x[1], int(x[4]))).reduceByKey(lambda x, y: x+y).map(lambda x: (int(x[1]),x[0]))
key_val_rdd.lookup(max_sal)
#['chennai']


#saveAstextfle
#rdd2.map(lambda x: (x[1], int(x[4]))).saveAsHadoopFile("hdfs:///user/hduser/city_salary.txt", "str")
rdd2.map(lambda x: (x[1], int(x[4]))).saveAsTextFile("file:///home/hduser/sparkdata/city_salary.txt")
rdd3= sc.textFile("file:///home/hduser/sparkdata/city_salary.txt")
rdd3.collect()


print("Optimization")

tild_rdd = sc.textFile("file:///home/hduser/sparkdata/tild_separate_file1")
tild_rdd.count()
#10654
tild_rdd.take(3)
#['8213034705~95~32.927373~jake7870~0~95~117.5~xbox~100', '8213034705~115~22.943484~davidbresler2~1~95~117.5~xbox~3', '8213034705~100~32.951285~gladimacowgirl~58~95~117.5~xbox~3']
tild_rdd.getNumPartitions()
#2
tild_rdd.repartition(4)
tild_rdd.getNumPartitions()
#2
# no effect in the above line num of partition because if you want to repartition you have to transform he rdd into new
# rdd for the new partitions
repartition_tild_rdd = tild_rdd.repartition(6)
repartition_tild_rdd.getNumPartitions()
#6
repartition_tild_rdd.map(lambda x: x.split("~")).take(3)

single_col_rdd = sc.textFile("file:///home/hduser/sparkdata/single_col_recs.txt")
#['A1', 'Apple', 'B2', 'Bangalore', 'C3', 'China', 'D4', 'Delhi']
zip_rdd=single_col_rdd.zipWithIndex()
#[('A1', 0), ('Apple', 1), ('B2', 2), ('Bangalore', 3), ('C3', 4), ('China', 5), ('D4', 6), ('Delhi', 7)]
even_lst = [0,2,4,6]
odd_lst = [1,3,5,7]
code_rdd=zip_rdd.filter(lambda x: x[1] in (0,2,4,6)).map(lambda x: x[0])
city_rdd=zip_rdd.filter(lambda x: x[1] in (1,3,5,7)).map(lambda x: x[0])
#code_rdd.union(city_rdd)
#code_rdd.join(code_rdd)
code_rdd.zip(city_rdd).collect()
fin_rdd = code_rdd.zip(city_rdd)
df = fin_rdd.toDF(["Code", "City"])
#df.write("E:\BigData\Shared_Documents\sample_data\sparkdata\double_cols")
#df.write.format("text")"E:\BigData\Shared_Documents\sample_data\sparkdata\double_cols")
#
course_rdd = sc.textFile("file:///home/hduser/sparkdata/course.log")
course_rdd.flatMap(lambda x: x.split(" ")).collect()
#['hadoop', 'spark', 'hadoop', 'spark', 'kafka', 'datascience', 'spark', 'hadoop', 'spark', 'datascience', 'informatica', 'java',
# 'aws', 'gcp', 'gcp', 'aws', 'azure', 'spark', 'gcp', 'pyspark', 'hadoop', 'hadoop']
course_rdd.map(lambda x: x.split(" ")).map(lambda x: (x[0],x[1])).collect()


########################################
print("Spark memory optimization")

log_analysis_rdd = sc.textFile("file:///home/hduser/sparkdata/error_warning_log")
log_analysis_rdd.count()
#450
warn_rdd = log_analysis_rdd.filter(lambda x: "WARN" in x)
#'2017-03-14 07:10:45,125 WARN org.apache.hadoop.util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable',
# '2017-03-14 09:30:47,284 WARN org.apache.hadoop.util.JvmPauseMonitor: Detected pause in JVM or host machine (eg GC): pause of approximately 56056ms',
err_rdd = log_analysis_rdd.filter(lambda x: "ERROR" in x)
#'2017-03-15 01:03:41,492 ERROR org.apache.hadoop.hdfs.server.datanode.DataNode: Inceptez:50010:DataXceiver error processing unknown operation  src: /127.0.0.1:53268 dst: /127.0.0.1:50010',
#'2017-03-21 07:28:40,456 ERROR org.apache.hadoop.hdfs.server.datanode.DataNode: RECEIVED SIGNAL 15: SIGTERM'
warn_rdd.count() #358
err_rdd.count() #90
other_rdd = log_analysis_rdd.filter(lambda x: "ERROR" not in x).filter(lambda x: "WARN" not in x)
########## other way ##############

log_analysis_rdd_split = log_analysis_rdd.map(lambda x: x.split(","))

range_rdd = sc.parallelize(range(1,1001))
range_rdd.count()
#1000
for i in range_rdd.glom().collect():
    #i.count()
    len(i)































