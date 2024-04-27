from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 5, 1, 3, 4, 6], 3)


    def process(iter):
        result = list()
        for i in iter:
            result.append(i)
        return result

    # 跟map功能一样,但是他读取数据是按分区来读,减少网络IO
    print(rdd.mapPartitions(process).collect())
