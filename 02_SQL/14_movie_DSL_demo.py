import time

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
        config("spark.sql.shuffle.partitions", "2"). \
        getOrCreate()
    sc = spark.sparkContext


    """
    "spark.sql.shuffle.partitions"参数指的是,在sql计算中,shuffle算子阶段默认的分区数是200个,
    对于集群模式来说200个默认也是比较合适的,
    如果在local下运行，200个很多,在调度上会带来损耗
    所以在local下建议修改较低,比如2\\4\\10都可
    
    """


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
    df.groupBy("user_id"). \
        avg("score"). \
        withColumnRenamed("avg(score)", "score_avg"). \
        orderBy("score_avg", ascending=False). \
        show()
    # TODO:查询电影平均分
    df.groupBy("mv_id").\
        avg("score").\
        withColumnRenamed("avg(score)", "score_avg").\
        orderBy("score_avg", ascending=False).\
        show()
    # TODO:查询大于平均分的电影
    avg_score = df.agg(
        F.avg("score")
    ).first()["avg(score)"]
    print(avg_score)
    df.groupBy("mv_id"). \
        avg("score"). \
        withColumnRenamed("avg(score)", "score_avg"). \
        where(F.col("score_avg") > avg_score).\
        orderBy("score_avg", ascending=False). \
        show()
    # TODO:查询高分电影中(>3)打分次数最多的用户,并求出此人打的平均分
    user_id = df.where("score > 3").\
        groupBy("user_id").\
        count().\
        withColumnRenamed("count", "user_id_count").\
        orderBy("user_id_count", ascending=False).\
        limit(1).\
        first()["user_id"]
    df.filter(df['user_id'] == user_id).\
        groupBy("user_id").\
        avg("score").\
        withColumnRenamed("avg(score)", "score_avg").\
        show()
    # TODO:查询每个用户打的平均分,最低打分,最高打分
    df.groupBy("user_id").\
        agg(
        F.avg("score").alias("score_avg"),
        F.min("score").alias("score_min"),
        F.max("score").alias("score_max")
    ).\
        show()
    # TODO:查询被评分超过100次的电影的平均分,
    df.groupBy("mv_id").\
        agg(
        F.count("mv_id").alias("cnt"),
        F.avg("score").alias("score_avg")
    ).\
        where("cnt > 100").show()


    df.select("user_id").show()
    time.sleep(100000)