from pyspark import SparkConf, SparkContext
import os

os.environ["HADOOP_CONF_DIR"] = "/export/server/hadoop-3.3.0/etc/hadoop"

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(0, 10))
    # a:当前数
    # b:上一个计算的结果
    # 是一个action算子
    result = rdd.reduce(lambda x, y: x + y)
    print(result)