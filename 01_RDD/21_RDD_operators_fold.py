from pyspark import SparkConf, SparkContext
import os

os.environ["HADOOP_CONF_DIR"] = "/export/server/hadoop-3.3.0/etc/hadoop"

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(1, 10), 3)
    # fold不常用
    # 可以有个初始值
    # 第一次执行时x为初始值
    # 但是他分区时会加上初始值,分区聚合时也会加上初始值(不常用)
    # 是一个action算子
    print(rdd.fold(10, lambda x, y: x + y))
