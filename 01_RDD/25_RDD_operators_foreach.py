from pyspark import SparkConf, SparkContext
import os

os.environ["HADOOP_CONF_DIR"] = "/export/server/hadoop-3.3.0/etc/hadoop"

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 5, 1, 3, 4, 6], 1)
    # collect是一并收集到driver中,儿foreach则是在各个executor中执行操作(并行)
    # 比较实用的案例是在各个executor中实现数据写入mysql
    # 是一个action算子
    rdd.foreach(lambda x: print(x))