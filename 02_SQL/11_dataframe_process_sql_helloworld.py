from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

if __name__ == '__main__':
    # 构建执行环境入口对象
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()
    sc = spark.sparkContext

    df = spark.read.format("csv"). \
        schema("id INT, subject STRING, score INT"). \
        load("../data/test_data/sql/stu_score.txt")

    df.createTempView("score")  # 注册临时视图(表)
    df.createOrReplaceTempView("score_2")  # 注册或者替换临时视图
    df.createGlobalTempView("score_3")  # 注册全局临视图,在使用时需要在前面带上global_temp. 前缀
    # 全局视图可在不同session使用
    spark.sql("select subject, count(*) as cnt from score group by subject").show()
    # 如果表名重复则会替换,不会报错
    spark.sql("select subject, count(*) as cnt from score_2 group by subject").show()
    spark.sql("select subject, count(*) as cnt from global_temp.score_3 group by subject").show()
