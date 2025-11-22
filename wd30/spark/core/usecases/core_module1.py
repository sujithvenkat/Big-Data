from pyspark.sql.session import SparkSession
from pyspark.storagelevel import StorageLevel


def main_method():
    print("In Main method")
    spark = SparkSession.builder.master("local[2]").appName("Venkadesh N S").getOrCreate()
    izsc = spark.sparkContext
    izsc.setLogLevel("ERROR")

    print("1. Create a hdfs filerdd from the file in a location /user/hduser/videolog/youtube_videos.tsv\n")
    filerdd = izsc.textFile("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\youtube_videos.tsv")

    print("2. Split the rows using tab (\"\\\\t\") delimiter\n")
    filerdd4part = filerdd.repartition(8)
    filerdd4part.persist(StorageLevel.MEMORY_AND_DISK_2)
    splitrdd = filerdd4part.map(lambda x: x.split("\t"))

    print("3. Remove the header record by filtering the first column value does not contains \"id\" into an rdd\n")
    # splitrdd or try using take/first/zipWithIndex function to remove the header
    # splitrdd.map(lambda x: x[0] if x[0] == 'id' else x).take(2)
    ''' 
    [['id', 'duration', 'bitrate', 'bitrate(video)', 'height', 'width', 'frame rate', 'frame rate(est.)', 'codec',
      'category', 'url']
    '''
    splitrdd = splitrdd.filter(lambda x: x[0] != 'id')
    ########## or #################
    # using take
    take_rdd_hdr = splitrdd.take(1)  # its just  a variable
    #   izsc.parallelize(take_rdd_hdr) --> now its a rdd
    splitrdd.filter(lambda x: x != take_rdd_hdr)  # not producing the expected results bcs take will take any one
    #   record not the first header record
    # using first
    first_rdd_hdr = splitrdd.first()
    hdr_rmvd = splitrdd.filter(lambda x: x != first_rdd_hdr)
    #   hdr_rmvd.filter(lambda x: x[0] == 'id')
    #################### or ###################
    # using zipWithIndex
    splitrdd.zipWithIndex().filter(lambda x: x[1] != '0').map(lambda x: x[0])
    # 0 should not be a string, its an index so its a number
    splitrdd.zipWithIndex().filter(lambda x: x[1] != 0).map(lambda x: x[0])

    print("4. Display only first 10 rows in the screen from splitrdd.\n")
    splitrdd.top(2)

    print("5. Filter only Music category data from splitrdd into an rdd called musicrdd \n")
    musicrdd = splitrdd.filter(lambda x: x[9] == 'Music')
    musicrdd.take(3)
    musicrdd = splitrdd.filter(lambda x: "Music" in x)
    # musicrdd.count()   #26512
    no_musicrdd = splitrdd.filter(lambda x: "Music" not in x)
    # people & blogs

    print("6. Filter only duration>100 data from splitrdd into an rdd called longdurrdd\n")
    # longdurrdd = splitrdd.filter(lambda x: x[1] > 100)
    # here duration has string value not a number value so need to put ""
    longdurrdd = splitrdd.filter(lambda x: x[1] > '100')
    longdurrdd.map(lambda x: (x[0], x[1])).take(2)
    # longdurrdd = splitrdd.map(lambda x: float(x[1])).filter(lambda x: x[1] > 100)
    # longdurrdd.map(lambda x: (x[0], x[1])).take(2)

    print("7. Union musicrdd with longdurrdd then convert to tuple and get only the deduplicated (distinct)\
records into an rdd music_longdur\n")
    music_longdur = longdurrdd.union(musicrdd).map(lambda x: tuple(x)).distinct()
    musicrdd.count()  # 26512
    longdurrdd.count()  # 166880
    music_longdur.count()  # before distinct 193392
    music_longdur.count()  # after distinct 166938

    print("8. Select only id, duration, codec and category by re-ordering the fields like \
id,category,codec,duration into an rdd mapcolsrdd\n")

    #    [['id', 'duration', 'bitrate', 'bitrate(video)', 'height', 'width', 'frame rate', 'frame rate(est.)', 'codec',
    #      'category', 'url']
    # mapcolsrdd = music_longdur.map(lambda x: (x[0], x[9], x[8], x[1]))
    mapcolsrdd_hdr = music_longdur.map(lambda x: (x[0], x[9], x[8], x[1]))
    mapcolsrdd_hdr.take(2)

    print("9. Select only duration column from mapcolsrdd and find max duration by using max function.\n")
    mapcolsrdd_hdr.map(lambda x: x[3]).max()  # it has headers now, so we have to remove it
    mapcolsrdd = mapcolsrdd_hdr.filter(lambda x: x[0] != 'id')
    mapcolsrdd.map(lambda x: x[3]).max()  # '999')

    print("10. Select only codec from mapcolsrdd, convert to upper case and print distinct of it in the screen.\n")
    # ['h264', 'h264']
    mapcolsrdd.map(lambda x: x[2].upper()).take(2)
    # ['H264', 'H264']

    print("11. Create an rdd called filerdd4part from filerdd created in step1 by increasing the number of\
partitions to 4 (Execute this step anywhere in the code where ever appropriate)\n")
    filerdd.getNumPartitions()  # 5
    filerdd4part = filerdd.repartition(8)
    filerdd4part.getNumPartitions()

    #    music_longdur.getNumPartitions()

    print("12. Persist the filerdd4part data into memory and disk with replica of 2, (Execute this step anywhere\
in the code where ever appropriate\n")

    filerdd4part.persist(StorageLevel.MEMORY_AND_DISK_2)

    print("13. Calculate and print the overall total, max, min duration for Comedy category\
id,category,codec,duration\n")
    comedy_cat = mapcolsrdd.filter(lambda x: x[1] == 'Comedy').map(lambda x: (x[1], x[3]))
    comedy_cat.take(8)
    # [('Comedy', '38'), ('Comedy', '233'), ('Comedy', '39'), ('Comedy', '39')]
    com_catgry_rdd_duration = comedy_cat.map(lambda x: float(x[1]))
    com_catgry_rdd_duration.cache()
    com_catgry_rdd_duration.sum()  # 1516889
    com_catgry_rdd_duration.take(2)
    com_catgry_rdd_duration.max()  # 7084.0
    com_catgry_rdd_duration.min()  # 2.0
    com_catgry_rdd_duration.unpersist()

    print("14. Print the codec wise count and minimum duration not by using min rather try to use reduce\
function  # id,category,codec,duration\n")
    codec_rdd = mapcolsrdd.map(lambda x: (x[2], x[3]))  # we are going to apply reduce by key so the value is should be int
    codec_rdd = mapcolsrdd.map(lambda x: (x[2], int(x[3])))
    codec_rdd.cache()
    codec_rdd.count()  # 166937
    codec_rdd.take(5)
    # [('h264', '1000'), ('vp8', '355'), ('h264', '341'), ('h264', '132'), ('vp8', '364')]
    # [('h264', 1000), ('vp8', 355), ('h264', 341), ('h264', 132), ('vp8', 364)]
    codec_wise_cnt = codec_rdd.reduceByKey(lambda x, y: x + y)
    codec_wise_cnt.persist(StorageLevel.MEMORY_AND_DISK_2)
    # codec_wise_cnt.foreach(print)
    # E:\spark-3.0.0-bin-hadoop2.7\spark-3.0.0-bin-hadoop2.7\python\lib\pyspark.zip\pyspark\shuffle.py:60: UserWarning: Please install psutil to have better support with spilling
    # ('none', 474)
    # ('h264', 20273947)
    # ('mpeg4', 6602255)
    # ('vp8', 12174225)
    # ('flv1', 6622324)
    print(codec_wise_cnt.collect())
    # [('vp8', 12174225), ('h264', 20273947), ('none', 474), ('mpeg4', 6602255), ('flv1', 6622324)]
    codec_rdd.map(lambda x: x[0]).distinct().count()  # 5
    # ['vp8', 'h264', 'none', 'mpeg4', 'flv1']
    codec_rdd.map(lambda x: x[0]).count()  # 166937

    #  Calculate min and max
    # min_codec_wise = min(codec_wise_cnt.map(lambda x: (x[1])))
    codec_wise_cnt.map(lambda x: x[1]).min()  # 474
    codec_wise_cnt = codec_wise_cnt.filter(lambda x: x[0] != 'none')
    codec_wise_cnt.cache()
    codec_wise_cnt.map(lambda x: x[1]).min()  # 6602255
    codec_wise_cnt.map(lambda x: x[1]).max()  # 20273947
    #  Calculate min and max without using min and max
    codec_wise_cnt.map(lambda x: x[1]).first()  # 12174225 it gave very first so we have to use top
    codec_wise_cnt.map(lambda x: x[1]).top()  # [20273947] it will give the desc order (max)
    # codec_wise_cnt.map(lambda x: x[1]).sortByKey().map(lambda x: x[0]).first()
    # All the key value transformation we need to give the input as a key value pair
    codec_wise_cnt.map(lambda x: (x[1], 1)).sortByKey().collect()
    codec_wise_cnt.map(lambda x: (x[1], 1)).sortByKey().first()  # (6602255, 1) it will give the asc order (min)
    codec_wise_cnt.map(lambda x: (x[1], 1)).sortByKey().map(lambda x: x[0]).first()  # 6602255 it will give the asc order (min)
    codec_wise_cnt.map(lambda x: (x[1], 1)).sortByKey(False).map(lambda x: x[0]).first()  # 20273947 t will give the des order (max)

    print("15. Print the distinct category of videos\n")
    # id,category,codec,duration
    mapcolsrdd.take(3)
    mapcolsrdd.map(lambda x: x[1]).distinct().count()  # 16
    mapcolsrdd.map(lambda x: x[1]).distinct().collect()

    print("16. Print only the id, duration, height, category and width sorted by duration")
    music_longdur.take(3)
    # [['id', 'duration', 'bitrate', 'bitrate(video)', 'height', 'width', 'frame rate', 'frame rate(est.)', 'codec',
    # 'category', 'url']
    music_longdur.map(lambda x: (x[1], x[0], x[4], x[9], x[5])).take(5)
    music_longdur.map(lambda x: (x[0][1], x[0], x[4], x[9], x[5])).take(5)
    # Here we are changing the column data as key value pair so that we can sort the key
    # Since we need to sort the data based on duration we are keeping duration as key and rest as value
    music_longdur.map(lambda x: (int(x[1]), (x[0], x[4], x[9], x[5]))).take(5)
    music_longdur.rdd
    music_longdur.map(lambda x: (int(x[1]), x[0], x[4], x[9], x[5])).take(5)
    # [(1000, ('dtMj-hglvaI', '640', 'Sports', '360')),
    # (355, ('EtV5-rrR4qU', '640', 'Nonprofits & Activis', '360')),
    # (341, ('fggR-s37mLc', '640', 'Music', '360')),
    # (132, ('hAOX-UH4VgY', '854', 'Gaming', '480')),
    # (364, ('Qhsr-FBpbWw', '1920', 'Entertainment', '1080'))]
    music_longdur.map(lambda x: (int(x[1]), (x[0], x[4], x[9], x[5]))).sortByKey().take(5)
    # [(2, ('e_Mq-YWkL5g', '1280', 'Pets & Animals', '720')),
    # (2, ('fSV0-l7znrc', '1920', 'People & Blogs', '1080')),
    # (2, ('fSV0-l7znrc', '640', 'People & Blogs', '360')),
    # (2, ('fSV0-l7znrc', '640', 'People & Blogs', '360')),
    # (2, ('FKJ6-vBd5lA', '480', 'Sports', '360'))]
    sorted_by_duration_mp = music_longdur.map(lambda x: (int(x[1]), (x[0], x[4], x[9], x[5]))).sortByKey()
    sorted_by_duration_mp.cache()
    sorted_by_duration_mp.map(lambda x: [str(x[0]), x[1][0], x[1][1], x[1][2], x[1][3]]).take(5)
    # [['2', 'e_Mq-YWkL5g', '1280', 'Pets & Animals', '720'],
    # ['2', 'fSV0-l7znrc', '1920', 'People & Blogs', '1080'],
    # ['2', 'fSV0-l7znrc', '640', 'People & Blogs', '360'],
    # ['2', 'fSV0-l7znrc', '640', 'People & Blogs', '360'],
    # ['2', 'FKJ6-vBd5lA', '480', 'Sports', '360']]

# ######### or #########

    # music_longdur.map(lambda x: (x[1], x[0], x[4], x[9], x[5])).sortByKey(lambda x: x[1]).take(5)
    # this sortbykey requires input as pair rdd but we didn't give that
    sorted_by_duration = music_longdur.map(lambda x: (int(x[1]), x[0], x[4], x[9], x[5])).sortBy(lambda x: x[0])
    sorted_by_duration.take(5)
    # [(2, 'e_Mq-YWkL5g', '1280', 'Pets & Animals', '720'),
    #  (2, 'fSV0-l7znrc', '1920', 'People & Blogs', '1080'),
    #  (2, 'fSV0-l7znrc', '640', 'People & Blogs', '360'),
    #  (2, 'fSV0-l7znrc', '640', 'People & Blogs', '360'),
    #  (2, 'FKJ6-vBd5lA', '480', 'Sports', '360')]
    '''
    # If we want to acheive this via sortbyKey then change the input to pairRDD,
    sorted_by_duration_other = sorted_by_duration_mp.map(lambda x: (x[0][0], x[1][0], x[0][4], x[0][9], x[0][5])).sortByKey(lambda x: int(x[0]))
    sorted_by_duration_other.take(5)
    sorted_by_duration = music_longdur.map(lambda x: (x[0][0], x[0][1], x[0][4], x[0][9], x[0][5])).sortBy(lambda x: x[1])
    sorted_by_duration.take(5)
    '''


    print("""17. Create a python function called masking which should take the string as input and returns the 
hash value of the input string.\n""")
    str_given = "Pyhon wth spark"
    print(f"The masked string is {masking(str_given)}")

    print("""18. Call the masking function created in the above step and pass category column and get the 
hashed value of category\n""")
    '''
    mapcolsrdd.take(5)
[('dtMj-hglvaI', 'Sports', 'h264', '1000'), ('EtV5-rrR4qU', 'Nonprofits & Activis', 'vp8', '355'), ('fggR-s37mLc', 'Music', 'h264', '341'), ('hAOX-UH4VgY', 'Gaming', 'h264', '132'), ('Qhsr-FBpbWw', 'Entertainment', 'vp8', '364')]
    
    sorted_by_duration.take(5)
    
    '''
    catgeory_column = sorted_by_duration.map(lambda x: (x[0], x[1], x[2], masking(x[3]), x[4]))
    print(f"The masked string is {catgeory_column.take(5)}")

    print("""19. Store the step 18 result in a hdfs location in a single file with data delimited as | with the id, 
duration, height, masking(category) and width columns""")

    catgeory_column.saveAsTextFile("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\cusecasefinal_op.txt")
# The above store the data in multiple files
    catgeory_column.map(lambda x: (x[1], str(x[0]), x[2], str(x[3]), x[4])).map(lambda x: '\t'.join(x)).take(5)
    catgeory_column.map(lambda x: (x[1], str(x[0]), x[2], str(x[3]), x[4])).map(lambda x: '|'.join(x)).coalesce(1).\
        saveAsTextFile("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\cusecasefinal_op_single_filepipe")


main_method()


def masking(str_val: str):
    print("The given string is " + str_val)
    print("The string is getting masked")
    hash(str_val)
    return str_val.__hash__()


'''
izsc = spark.sparkContext
filerdd = izsc.textFile("E:\\BigData\\Shared_Documents\\sample_data\\sparkdata\\youtube_videos.tsv")
filerdd4part = filerdd.repartition(8)
from pyspark.storagelevel import StorageLevel
filerdd4part.persist(StorageLevel.MEMORY_AND_DISK_2)
splitrdd = filerdd4part.map(lambda x: x.split("\t"))
splitrdd = splitrdd.filter(lambda x: x[0] != 'id')
musicrdd = splitrdd.filter(lambda x: x[9] == 'Music')
longdurrdd = splitrdd.filter(lambda x: x[1] > '100')
music_longdur = longdurrdd.union(musicrdd).map(lambda x: tuple(x)).distinct()
mapcolsrdd = music_longdur.map(lambda x: (x[0], x[9], x[8], x[1]))
comedy_cat = mapcolsrdd.filter(lambda x: x[1] == 'Comedy').map(lambda x: (x[1], x[3]))
codec_rdd = mapcolsrdd.map(lambda x: (x[2], int(x[3])))
sorted_by_duration = music_longdur.map(lambda x: (int(x[1]), (x[0], x[4], x[9], x[5]))).sortByKey()
#or
sorted_by_duration = music_longdur.map(lambda x: (int(x[1]), x[0], x[4], x[9], x[5])).sortBy(lambda x: x[0])
'''

'''
######## testing purpose #####################################
    #map(lambda x, y: (x, y[0], y[1], y[2], y[3]))
    sort_by_duration_keys = sorted_by_duration.keys()
    # list of integers we have to convert to list string rdd
    #sort_by_duration_keys_str = map(str, sort_by_duration_keys)  -> this for normal python not rdd
    sort_by_duration_keys_str = sort_by_duration_keys.map(lambda x: str(x))
    sorted_by_duration_values = sorted_by_duration.values()
    #sort_by_duration = sort_by_duration_keys_str.zip(sorted_by_duration_values)
    sort_by_duration = sort_by_duration_keys_str.union(sorted_by_duration_values)
    #[
    #('2', ('e_Mq-YWkL5g', '1280', 'Pets & Animals', '720')),
    #('2', ('fSV0-l7znrc', '1920', 'People & Blogs', '1080')),
    #('2', ('fSV0-l7znrc', '640', 'People & Blogs', '360')),
    #('2', ('fSV0-l7znrc', '640', 'People & Blogs', '360')),
    #('2', ('FKJ6-vBd5lA', '480', 'Sports', '360'))
    # ]
    #sort_by_duration.map(lambda x, y: (x, y[0][1])).take(2)

    sort_by_duration_keys_str.count()  # 166937
    sorted_by_duration_values.count()  # 166937
    sort_by_duration = sort_by_duration_keys_str.join(sorted_by_duration_values.map(lambda x: (x[0], x[1:])))

# test
    #rdd_1 = izsc.parallelize(list('2', '3', '4', '5', '6'))
    rdd_1 = izsc.parallelize(list(('2', '3', '4', '5', '6')))
    rdd_2 = izsc.parallelize(list((('e_Mq-YWkL5g', '1280', 'Pets & Animals', '720'),
                                  ('fSV0-l7znrc', '1920', 'People & Blogs', '1080'),
                                   ('fSV0-l7znrc', '640', 'People & Blogs', '360'),
                                   ('fSV0-l7znrc', '640', 'People & Blogs', '360'),
                                   ('FKJ6-vBd5lA', '480', 'Sports', '360'))))
#>>> rdd_1.collect()
#['2', '3', '4', '5', '6']

#>>> rdd_2.collect()
#[('e_Mq-YWkL5g', '1280', 'Pets & Animals', '720'), ('fSV0-l7znrc', '1920', 'People & Blogs', '1080'), ('fSV0-l7znrc', '640', 'People & Blogs', '360'), ('fSV0-l7znrc', '640', 'People & Blogs', '360'), ('FKJ6-vBd5lA', '480', 'Sports', '360')]

    rdd_1.join(rdd_2) # string index out of range
    rdd_1 + rdd_2.collect() # raise TypeError
    rdd_1.zip(rdd_2).collect()
    rdd_1.zip(rdd_2).map(lambda x: (x[0], x[1])).collect()
    rdd_2_list = rdd_2.map(lambda x: [x[0], x[1], x[2], x[3]])
    ziped_rdd = rdd_1.zip(rdd_2_list)
    ziped_rdd.cache()
    ziped_rdd.map(lambda x: [x[0], x[1]]).map(lambda x: x.split(",")).collect()

#[('2', ('e_Mq-YWkL5g', '1280', 'Pets & Animals', '720')),
# ('3', ('fSV0-l7znrc', '1920', 'People & Blogs', '1080')),
# ('4', ('fSV0-l7znrc', '640', 'People & Blogs', '360')),
# ('5', ('fSV0-l7znrc', '640', 'People & Blogs', '360')),
# ('6', ('FKJ6-vBd5lA', '480', 'Sports', '360'))]
    rdd_1_map = rdd_1.map(lambda x: (x, 1))
    rdd_1_map.collect()
    rdd_1_zip = rdd_1.zipWithIndex()
    rdd_1_zip.collect()
    rdd_2_zip = rdd_2.zipWithIndex()
    rdd_2_zip.collect()


    rdd_1_zip.zip(rdd_2_zip).map(lambda x: x.split(",")).collect()(lambda x, y: x + y).collect()

    rdd_1_map.map(lambda x: x).collect()
    rdd_2_map = rdd_2.map(lambda x: (x, 1))
    rdd_2_map.collect()
    rdd_1.zip(rdd_2).map(lambda x: x[0]).collect()
    #['2', '3', '4', '5', '6']
    rdd_1_map.zip(rdd_2_map).map(lambda x: x[0]).collect()
    rdd_1.zip(rdd_2).map(lambda x, y: (x[0], y[0], y[1], y[2], y[3])).collect()
    fine_rdd = rdd_1_map + rdd_1_map
'''

