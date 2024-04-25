from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1), ("b", 1), ("b", 1)])

    # 通过groupBy对数据进行分组
    # groupBy传入的函数的意思是,根据谁来进行分组
    result = rdd.groupBy(lambda t: t[0])
    print(result.map(lambda t: (t[0], list(t[1]))).collect())
