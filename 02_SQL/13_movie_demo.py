from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import functions as F
import re

if __name__ == '__main__':

    def split_data(data):
        result = re.split(r"\s+", data)
        return [result[0], result[1], int(result[2]), result[3]]
    # 构建执行环境入口对象
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()
    sc = spark.sparkContext
    # TODO 数据准备
    # 读取数据
    rdd = sc.textFile("../data/test_data/sql/u.data")
    # 数据处理
    rdd = rdd.map(split_data)
    # 创建表格描述
    schema = StructType(). \
        add("user_id", StringType()). \
        add("mv_id", StringType()). \
        add("score", IntegerType()). \
        add("time", StringType())
    df = rdd.toDF(schema=schema)
    df.limit(5).show()
    # 注册成临时表
    df.createTempView("movie_data")
    # TODO:查询用户平均分
    spark.sql("""
        select user_id, avg(score) from movie_data group by user_id
    """).show()
    # TODO:查询电影平均分
    spark.sql("""
        select mv_id, avg(score) from movie_data group by mv_id
    """).show()
    # TODO:查询大于平均分的电影
    spark.sql("""
        select mv_id
        from movie_data 
        where score > (
            select avg(score) 
            from movie_data
        ) 
        group by mv_id
    """).show()
    # TODO:查询高分电影中(>3)打分次数最多的用户,并求出此人打的平均分

    # TODO:查询每个用户打的平均分,最低打分,最高打分
    # TODO:查询被评分超过100次的电影的平均分,排名top10
