import time

from pyspark import SparkConf, SparkContext
import re

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    char = [",", ".", "!", "#", "$", "%"]
    broadcast = sc.broadcast(char)
    acmlt = sc.accumulator(0)


    def filet_word(data):
        global acmlt
        if data in char:
            acmlt += 1
            return False
        return True


    rdd = sc.textFile("../data/test_data/accumulator_broadcast_data.txt")
    rdd = rdd.filter(lambda x: x.strip())
    rdd = rdd.map(lambda x: x.strip())
    rdd = rdd.flatMap(lambda x: re.split(r"\s+", x))
    rdd = rdd.filter(filet_word)
    rdd = rdd.map(lambda x: (x, 1))
    rdd = rdd.reduceByKey(lambda a, b: a + b)
    print(rdd.collect())
    print(acmlt)