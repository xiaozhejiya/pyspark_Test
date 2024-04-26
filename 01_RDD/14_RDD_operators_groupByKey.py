from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([("a", 1), ("a", 1), ("b", 1), ("b", 1), ("b", 1)])

    # groupByKey:根据键来分组
    # 与groupBy不同,groupBy会保存键(这点和scala的groupBy相同)
    # 与reduceByKey不同,他不会i进行聚合操作
    print(rdd.groupByKey().map(lambda x: (x[0], list(x[1]))).collect())
