import time

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    stu_info_list = {1: ("张大仙", 11), 2: ("王晓晓", 11), 3: ("张甜甜", 11), 4: ("王大力", 11)}
    # 广播变量:
    # 减少网络IO,减少executor内存占用
    broadcast = sc.broadcast(stu_info_list)
    rdd = sc.parallelize([
        (1, "语文", 99),
        (2, "数学", 99),
        (3, "英语", 99),
        (4, "编程", 99),
        (1, "语文", 99),
        (2, "数学", 99),
        (3, "英语", 99),
        (4, "编程", 99),
        (1, "语文", 99),
        (2, "数学", 99),
        (3, "英语", 99),
        (4, "编程", 99)
    ])


    def map_func(data):
        id = data[0]
        name = ""
        if id in broadcast.value:
            name = broadcast.value[1][0]
        return (name, data[1], data[2])


    print(rdd.map(map_func).collect())