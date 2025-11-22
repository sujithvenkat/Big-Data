import os

from pyspark import StorageLevel

os.environ["PYSPARK_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"

from pyspark.sql import SparkSession
def masking(str_to_mask):
    return hash(str_to_mask)
def main():
    spark=SparkSession.builder.appName("Venky").master('local[2]').getOrCreate()
    izsc=spark.sparkContext
    izsc.setLogLevel("Error")
    print(izsc.appName)

    file_rdd=izsc.textFile("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\youtube_videos.tsv")
    hdr_rdd1 = file_rdd.first()
    file_rdd4part = file_rdd.repartition(8)
    file_rdd_persist=file_rdd4part.persist(StorageLevel.MEMORY_AND_DISK_2)
    file_split_rdd=file_rdd_persist.map(lambda row:row.split('\t'))
    rem_hdr_rdd=file_split_rdd.filter(lambda row:row[0] != 'id')
    # hdr_rdd=file_split_rdd.take(1)[0]

    # hdr_rdd2=file_split_rdd.zipWithIndex().map(lambda x:x[0]).take(1)
    # rem_hdr_rdd2=file_split_rdd.filter(lambda rec:rec != hdr_rdd1)
    print("header : ", hdr_rdd1)
    print("Sample records", rem_hdr_rdd.take(4))
    music_rdd=rem_hdr_rdd.filter(lambda x:x[9] == 'Music')
    longdur_rdd= rem_hdr_rdd.filter(lambda x: int(x[1]) > 100)
    music_longdur_bef=music_rdd.union(longdur_rdd).map(lambda x:tuple(x))
    music_longdur=music_longdur_bef.distinct()
    # print(f"counts for music {music_rdd.count()} and for duration rdd's {longdur_rdd.count()} and "
    #       f"finally after union music_longdur count {music_longdur_bef.count()} and after distinct {music_longdur.count()}")
    mapcolsrdd=music_longdur.map(lambda x:[x[0],x[9],x[8],x[1]])
    max_dur=mapcolsrdd.map(lambda x:int(x[3])).max()
    uniq_codec=mapcolsrdd.map(lambda x:x[2].upper()).distinct()
    durations=mapcolsrdd.filter(lambda x:x[1] == 'Comedy').map(lambda x:int(x[3]))
    max_dur_comedy=durations.max()
    min_dur_comedy = durations.min()
    total_dur_comedy= durations.sum()
    codec_count=mapcolsrdd.map(lambda x:(x[2],1)).reduceByKey(lambda x,y:x+y)
    codec_min=mapcolsrdd.map(lambda x:(x[2],int(x[3]))).reduceByKey(lambda a,b:a if a > b else b)
    distinct_video=mapcolsrdd.map(lambda x:x[1]).distinct()
    sorted_select_cols=music_longdur.map(lambda x:[x[0],x[1],x[4],x[9],x[5]]).sortBy(lambda x:x[1])
    hashing_category=sorted_select_cols.map(lambda x:[x[0],x[1],x[2],masking(x[3]),x[4]])
    map_final_rdd=hashing_category.map(lambda x: "|".join(map(str,x)))
    map_final_rdd.coalesce(1).saveAsTextFile("file:\\E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\results_2025.txt")


main()




