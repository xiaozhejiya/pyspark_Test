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

    schema = StructType().add("user_id", StringType(), nullable=True). \
        add("movie_id", IntegerType(), nullable=True). \
        add("rank", IntegerType(), nullable=True). \
        add("ts", StringType(), nullable=True)
    df = spark.read.format("csv"). \
        option("sep", "\t"). \
        option("header", False). \
        option("encoding", "utf-8"). \
        schema(schema=schema). \
        load("../data/test_data/sql/u.data")

    # 写入数据到JDBC
    # df.write.mode("overwrite"). \
    #     format("jdbc"). \
    #     option("url", "jdbc:mysql://10.3.15.116:3306/bigdata?useSSL=false&useUnicode=true"). \
    #     option("dbtable", "movie_data"). \
    #     option("user", "root"). \
    #     option("password", "hadoop").\
    #     save()
    # 从JDBC读取数据
    df2 = spark.read.format("jdbc"). \
        option("url", "jdbc:mysql://10.3.15.116:3306/bigdata?useSSL=false&useUnicode=true"). \
        option("dbtable", "movie_data"). \
        option("user", "root"). \
        option("password", "hadoop").\
        load()
    df2.select
    df2.show()