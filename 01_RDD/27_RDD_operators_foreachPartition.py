from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 5, 1, 3, 4, 6], 3)


    def process(iter):
        result = list()
        for i in iter:
            result.append(i)
        print(result)


    rdd.foreachPartition(process)
