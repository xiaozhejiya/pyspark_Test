from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(1, 7))

    result = rdd.filter(lambda x: x % 2 == 0)
    print(result.collect())
