from pyspark import SparkConf, SparkContext


if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 5, 1, 3, 4, 6], 3)
    # 有executor执行
    rdd.saveAsTextFile("../data/output/output2")