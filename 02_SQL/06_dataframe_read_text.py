from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

if __name__ == '__main__':
    # 构建执行环境入口对象
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    schema = StructType().add("data", StringType(), nullable=True)
    df = spark.read.format("text"). \
        schema(schema=schema). \
        load("../data/test_data/sql/people.txt")

    df.printSchema()
    df.show()