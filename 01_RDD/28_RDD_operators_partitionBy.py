from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([("hadoop", 1), ("spark", 1), ("hello", 1), ("hadoop", 1), ("flink", 2)])


    def process(k):
        return ord(k[0]) % 3

    # 参数一:分区数
    # 参数二:要求返回值是个分区(int)
    print(rdd.partitionBy(3, process).glom().collect())
