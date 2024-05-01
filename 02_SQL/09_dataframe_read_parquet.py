from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

if __name__ == '__main__':
    # 构建执行环境入口对象
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    df = spark.read.format("parquet").load("../data/test_data/sql/users.parquet")

    df.printSchema()
    df.show()