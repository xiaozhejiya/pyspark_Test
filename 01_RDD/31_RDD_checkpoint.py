import time

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setCheckpointDir("hdfs://node5:8020/output/ckp")

    rdd = sc.textFile("hdfs://node5:8020/word.txt")

    rdd2 = rdd.flatMap(lambda x: x.split(" "))
    rdd3 = rdd2.map(lambda x: (x, 1))
    # 设计上是安全的,执行大任务时使用
    rdd3.checkpoint()

    rdd4 = rdd3.reduceByKey(lambda a, b: a + b)

    print(rdd4.collect())

    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x: sum(x))
    print(rdd6.collect())

    rdd3.unpersist()
    time.sleep(1000)
