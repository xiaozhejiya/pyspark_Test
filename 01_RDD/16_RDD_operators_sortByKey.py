from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize(
        [('A', 1), ('B', 2), ('c', 3), ('d', 4), ('e', 5), ('f', 6), ('G', 7), ('h', 8), ('i', 9), ('j', 10),
         ('K', 11), ('l', 12), ('m', 13), ('n', 14), ('o', 15), ('p', 16), ('q', 17), ('R', 18), ('S', 19),
         ('t', 20), ('u', 21), ('v', 22), ('w', 23), ('x', 24), ('Y', 25), ('Z', 26)]
        )
    # 根据键的键来进行排序
    print(rdd.sortByKey(ascending=True, numPartitions=1, keyfunc=lambda x: str(x).lower()).collect())
