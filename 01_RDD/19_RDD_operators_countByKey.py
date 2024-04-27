from pyspark import SparkConf, SparkContext
import os

os.environ["HADOOP_CONF_DIR"] = "/export/server/hadoop-3.3.0/etc/hadoop"

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("hdfs://node5:8020/word.txt")
    rdd = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1))
    # 是一个action算子,对key进行计数
    result = rdd.countByKey()
    print(result)