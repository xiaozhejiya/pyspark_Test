import string

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
from pyspark.sql import functions as F

if __name__ == '__main__':
    # 构建执行环境入口对象
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()
    sc = spark.sparkContext
    rdd = sc.parallelize([[1], [2], [3]])
    df = rdd.toDF(["num"])

    def process(data):
        return {"num": data, "letters": string.ascii_letters[data]}

    udf2 = spark.udf.register("udf1", process, StructType().add("num", IntegerType()).add("letters", StringType()))
    df.select(udf2(df["num"])).show()

    # sql方式
    df.createTempView("tmp_table")
    spark.sql("select udf1(num) from tmp_table").show()


    # 自定义函数二
    udf3 = F.udf(process, StructType().add("num", IntegerType()).add("letters", StringType()))
    df.select(udf3(df["num"])).show()
