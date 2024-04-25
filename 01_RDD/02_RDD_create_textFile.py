from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile("../data/input/words.txt")
    print(rdd.getNumPartitions())

    # 读取hdfs
    rdd2 = sc.textFile("hdfs://10.3.15.116:8020/word.txt", 2)
    print("rdd2:", rdd2.getNumPartitions())

    # 适合用于读取小文件
    rdd3 = sc.wholeTextFiles("hdfs://10.3.15.116:8020/word.txt", 2)
    print("rdd3:", rdd3.collect())
