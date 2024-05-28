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
    rdd = sc.parallelize(range(1, 10)).map(lambda x: [x])
    df = rdd.toDF(["num"])
    def num_ride_10(num):
        return num * 10
    # 其中参数udf1是提供给sql方式使用
    # 返回值udf2是提供给dsl使用
    udf2 = spark.udf.register("udf1", num_ride_10, IntegerType())
    # 用方法用于执行UDF
    df.selectExpr("udf1(num)").show()
    # dsl使用
    df.select(udf2(df["num"])).show()

    # TODO:UDF方式二
    # 只能dsl使用
    udf3 = F.udf(num_ride_10, IntegerType())
    df.select(udf3(df["num"])).show()
