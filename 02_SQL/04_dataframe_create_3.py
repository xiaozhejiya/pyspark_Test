from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    # 构建执行环境入口对象
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()
    sc = spark.sparkContext
    # 基于RDD转换成DataFrame
    rdd = sc.textFile("../data/test_data/sql/people.txt"). \
        map(lambda x: x.split(",")). \
        map(lambda x: (x[0], int(x[1])))

    # toDF方式构建DataFrame
    df1 = rdd.toDF(["name", "age"])
    df1.printSchema()
    df1.show()

    # 方式二:
    schema = StructType()\
        .add("name", StringType(), nullable=False)\
        .add("age", IntegerType(), nullable=True)

    df2 = rdd.toDF(schema=schema)
    df2.printSchema()
