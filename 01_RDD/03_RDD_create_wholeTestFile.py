from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 读取小文件
    rdd = sc.wholeTextFiles("../data/test_data/tiny_files")
    print(rdd.map(lambda x: x[1]).collect())
