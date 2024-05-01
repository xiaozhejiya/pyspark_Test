from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    # 构建执行环境入口对象
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext
    # 基于RDD转换成DataFrame
    rdd = sc.textFile("../data/test_data/sql/people.txt").\
        map(lambda x: x.split(",")).\
        map(lambda x: (x[0], int(x[1])))

    # 构建表结构的描述对象: StructType
    schema = StructType().\
        add("name", StringType(), nullable=True).\
        add("age", IntegerType(), nullable=False)
    # 基于StructType对象去构建RDD到DF的转换
    df = spark.createDataFrame(rdd, schema=schema)

    df.printSchema()
    df.show()
