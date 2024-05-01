from pyspark import SparkConf, SparkContext
import re

if __name__ == '__main__':
    conf = SparkConf().setAppName("WordCount")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile("hdfs://node5:8020/lsh/lsh.txt", minPartitions=1)

    rdd.flatMap(lambda data: re.split(r"[,\s]+", data)). \
        map(lambda x: (x, 1)). \
        reduceByKey(lambda x, y: x + y). \
        saveAsTextFile("hdfs://node5:8020/output/WortCountResult")
