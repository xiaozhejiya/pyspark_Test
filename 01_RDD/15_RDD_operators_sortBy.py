from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5)])
    # 根据指定的字段排序
    """
    参数1,表示排序的字段
    参数2,表示TRUE表示升序
    参数3,表示排序分区数
    ----注意: 如果要全局有序排序的分区数应该设置为1---因为executor是并行执行,多个分区只能局部排序
    """
    print(rdd.sortBy(lambda x: x[1], ascending=False, numPartitions=1).collect())
    print(rdd.sortBy(lambda x: x[0]).collect())
