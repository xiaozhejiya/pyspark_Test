from pyspark import SparkConf, SparkContext
if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 并行化集合创建RDD
    rdd = sc.parallelize(range(10))
    # 查看分区数
    print(rdd.getNumPartitions())