from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
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
    rdd = sc.parallelize([["spark flink java"], ["python C++ numpy"]])


    def split_line(data):

        return data.split(" ")

    udf2 = spark.udf.register("udf1", split_line, ArrayType(StringType()))

    # sql形式
    df = rdd.toDF(["line"])
    # 注册成临时表
    df.createTempView("lines")
    spark.sql("select udf1(line) from lines").show(truncate=False)  # 将结果全部显示不隐藏

    # dsl形式
    df.select(udf2(df["line"])).show()

    # 方式二构建udf
    udf3 = F.udf(split_line, ArrayType(StringType()))
    df.select(udf3(df["line"])).show()