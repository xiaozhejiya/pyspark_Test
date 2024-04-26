from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([("a", 1), ("a", 3)])
    rdd2 = sc.parallelize([("a", 1), ("b", 3)])

    # 返回RDD的交集
    print(rdd1.intersection(rdd2).collect())