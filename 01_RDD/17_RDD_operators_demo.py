from pyspark import SparkConf, SparkContext
import json


if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("../data/test_data/order.text")
    rdd = rdd.flatMap(lambda x: x.split("|"))
    rdd = rdd.map(lambda x: json.loads(x))
    rdd = rdd.filter(lambda x: x["areaName"] == "北京")
    rdd = rdd.map(lambda x: (x["category"], x["areaName"]))
    rdd = rdd.distinct()
    print(rdd.collect())
