from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("AllTransformationsDemo").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # Base RDD
    rdd = sc.parallelize([1, 2, 3, 4, 5, 5, 6, 7])

    print("===== map() - transform each record =====")
    print(rdd.map(lambda x: x * 2).collect())  # [2,4,6,8,10,10,12,14]

    print("===== flatMap() - emit multiple records =====")
    words = sc.parallelize(["hello world", "spark rdd"])
    print(words.flatMap(lambda x: x.split(" ")).collect())  # ['hello','world','spark','rdd']

    print("===== filter() - keep only matching rows =====")
    print(rdd.filter(lambda x: x % 2 == 0).collect())  # [2,4,6]

    print("===== mapPartitions() - apply logic per partition =====")
    def process_partition(iterator):
        yield sum(iterator)
    print(rdd.mapPartitions(process_partition).collect())   # sums of each partition

    print("===== distinct() - remove duplicates (shuffle) =====")
    print(rdd.distinct().collect())  # [1,2,3,4,5,6,7]

    print("===== union() =====")
    rdd2 = sc.parallelize([8, 9])
    print(rdd.union(rdd2).collect())  # combined results

    # Pair RDD
    pair = sc.parallelize([("a", 1), ("b", 2), ("a", 3), ("b", 1), ("c", 5)])

    print("===== join() - combine based on keys =====")
    pair2 = sc.parallelize([("a", 10), ("b", 20)])
    print(pair.join(pair2).collect())  # [('a',(1,10)),('a',(3,10)),('b',(2,20)),('b',(1,20))]

    print("===== reduceByKey() - efficient aggregation =====")
    print(pair.reduceByKey(lambda a, b: a + b).collect())  # [('a',4),('b',3),('c',5)]

    print("===== aggregateByKey() - different local/global ops =====")
    print(pair.aggregateByKey(0, lambda x, y: x + y, lambda x, y: x + y).collect())

    print("===== combineByKey() - most flexible aggregation =====")
    combine_result = pair.combineByKey(
        lambda v: (v, 1),               # initial
        lambda acc, v: (acc[0] + v, acc[1] + 1),  # merge per partition
        lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # merge partitions
    )
    print(combine_result.mapValues(lambda x: x[0] / x[1]).collect())  # avg per key

    print("===== groupByKey() - avoid unless needed (heavy shuffle) =====")
    print(pair.groupByKey().mapValues(list).collect())

    print("===== reduce() - single output aggregate =====")
    print(rdd.reduce(lambda a, b: a + b))  # sum of all

    print("===== count() vs countByValue() =====")
    print(rdd.count())                # 8
    print(rdd.countByValue())         # freq of each value

    print("===== repartition() =====")
    print(rdd.repartition(4).getNumPartitions())

    print("===== coalesce() - reduce partitions =====")
    print(rdd.coalesce(1).getNumPartitions())

    print("===== sortByKey() =====")
    print(pair.sortByKey().collect())  # sort by key

    sc.stop()
