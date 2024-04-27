from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 5, 1, 3, 4, 6], 3)
    # 修改分区数
    # 不推荐,容易造成多次shuffle
    print(rdd.repartition(1).getNumPartitions())
    print(rdd.repartition(5).getNumPartitions())
    # coalesce修改分区\
    # 参数二:是否运行shuffle
    print(rdd.coalesce(1).getNumPartitions())
    print(rdd.coalesce(5).getNumPartitions())

    # 面试题:reduceByKey和groupByKey的区别
