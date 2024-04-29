
from pyspark import SparkConf, SparkContext
import os

os.environ["HADOOP_CONF_DIR"] = "/export/server/hadoop-3.3.0/etc/hadoop"

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(1, 10), 3)
    # 是一个action算子
    # 返回第一个
    print(rdd.first())
    # 返回前n个
    print(rdd.take(5))
    # 降序排序返回前n个
    print(rdd.top(3))
    # 返回数据的个数
    print(rdd.count())
