from pyspark import SparkConf, SparkContext
import os
from defs import *
from pyspark.storagelevel import StorageLevel
from operator import add

os.environ["HADOOP_CONF_DIR"] = "/export/server/hadoop-3.3.0/etc/hadoop"

if __name__ == '__main__':
    conf = SparkConf().setAppName("user_log").setMaster("yarn")
    conf.set("spark.submit.pyFiles", "defs.py")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("hdfs://node5:8020/input/SogouQ.txt")

    rdd_memory = rdd.map(lambda x: x.split("\t"))

    # 缓存到磁盘中
    rdd_memory.persist(storageLevel=StorageLevel.MEMORY_ONLY)

    rdd = rdd_memory.map(lambda x: x[2])

    # 分词
    rdd = rdd.flatMap(split_word)

    # 过滤
    rdd = rdd.filter(filter_words)
    # 转换关键词
    rdd = rdd.map(append_words)

    # 对单词进行分组聚合,排序,取出前五名
    result = rdd.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(5)
    print(result)

    # 任务二分析用户个人所好
    # 统计某个用户最常搜索词
    rdd = rdd_memory.map(lambda x: (x[1], x[2]))
    result = rdd.flatMap(aggregation_user_count)
    print(result.take(20))

    result = result.reduceByKey(lambda a, b: a + b). \
        sortBy(lambda x: x[1], ascending=False, numPartitions=1). \
        take(5)

    print(result)

    # 任务三热门搜索时间段分析
    # 取出时间
    rdd = rdd_memory.map(lambda x: x[0])
    # # 根据时间段进行分组0 - 1
    # rdd = rdd.groupBy(lambda x: x[:2])
    # rdd = rdd.map(lambda x: (x[0], len(x[1])))
    # result = rdd.sortBy(lambda x: x[1], ascending=False, numPartitions=1)
    # print(result.collect())

    # 方式二
    rdd = rdd.map(lambda x: (x.split(":")[0], 1))
    result = rdd.reduceByKey(add). \
        sortBy(lambda x: x[1], ascending=False, numPartitions=1). \
        collect()
    print(result)
