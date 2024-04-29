import time

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(1, 11), 2)

    # 累加器 参数是初始值
    acmlt = sc.accumulator(0)

    def map_func(data):
        global acmlt
        acmlt += 1

    rdd2 = rdd.map(map_func)
    rdd2.cache()
    # 为了避免rdd2调用操作方法而销毁,需要把rdd2缓存到内存当中(避免累加器被rdd2多次执行)
    rdd2.collect()

    rdd3 = rdd2.map(lambda x: x)
    print(acmlt)

