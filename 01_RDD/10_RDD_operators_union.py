from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd1 = sc.parallelize(range(0, 4))
    rdd2 = sc.parallelize(["a", "b", "c"])
    # 合并算子
    rdd3 = rdd1.union(rdd2)
    print(rdd3.collect())
