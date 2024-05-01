from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

if __name__ == '__main__':
    # 构建执行环境入口对象
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    df = spark.read.format("csv").\
        option("sep", ";"). \
        option("header", True). \
        option("encoding", "utf-8"). \
        schema("name STRING, age INT, job STRING").\
        load("../data/test_data/sql/people.csv")

    df.printSchema()
    df.show()