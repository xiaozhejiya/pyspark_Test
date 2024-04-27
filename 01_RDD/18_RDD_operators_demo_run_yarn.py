from pyspark import SparkConf, SparkContext
import json
import os
from defs_18 import city_with_category
# 提交到集群中需要指定hadoop的位置
# 另外还有可能会因为用户权限问题出现报错,需要去到Linux系统给予最高权限
os.environ["HADOOP_CONF_DIR"] = "/export/server/hadoop-3.3.0/etc/hadoop"
# 提交到集群与运行
# spark-submit --master yarn --py-files ./defs.py /export/software/my_code/codePy/main.py
if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("yarn")
    # 如果提交到集群运行,除了主代码外,还依赖了其他的代码文件
    # 需要设置一个参数,来告诉spark,还有依赖文件要同步到集群中
    # spark.submit.pyFiles
    # 参数可以是.py 和 .zip文件(多个依赖)
    conf.set("spark.submit.pyFiles", "defs_18.py")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("hdfs://node5:8020/input/order.text")
    rdd = rdd.flatMap(lambda x: x.split("|"))
    rdd = rdd.map(lambda x: json.loads(x))
    rdd = rdd.filter(lambda x: x["areaName"] == "北京")
    rdd = rdd.map(city_with_category)
    rdd = rdd.distinct()
    print(rdd.collect())
