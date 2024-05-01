from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 构建执行环境入口对象
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext
    # 基于RDD转换成DataFrame
    rdd = sc.textFile("../data/test_data/sql/people.txt").\
        map(lambda x: x.split(",")).\
        map(lambda x: (x[0], int(x[1])))
    # 构建DataFrame对象
    # 参数1: 被转换的RDD
    # 参数2: 指定列名字,通过LIST的形式指定,按照顺序依次提供字符串名称即可
    df = spark.createDataFrame(rdd, schema=["name", "age"])
    # 打印DataFrame的表结构
    df.printSchema()

    # 打印df的数据
    # 参数1: 表示展出多少条数据,不传默认20
    # 参数2: 表示是否对数据进行截断,如果列的数据长度超过20个字符串的长度,后续内容不显示以...代替
    # 如果给False表不截断全部显示,默认是True
    df.show(20, False)
    # 将DF对象转换成临时视图表,
    #
    #  可用SQL查询
    df.createTempView("people")
    spark.sql("select * from people where age< 30").show()