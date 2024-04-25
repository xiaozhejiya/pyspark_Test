from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("WordCount")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile("hdfs://node5:8020/word.txt") \
        .flatMap(lambda x: x.split(" ")) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b)

    print(rdd.collect())
