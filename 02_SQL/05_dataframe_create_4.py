from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd

if __name__ == '__main__':
    # 构建执行环境入口对象
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()
    sc = spark.sparkContext
    # pandas的dataFrame构建SparkSql的dataFrame

    pdf = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["张三", "李四", "王五"],
        "age": [11, 21, 11]
    })
    # 定义 schema
    schema = StructType().add("id", StringType()).add("name", StringType()).add("age", IntegerType())
    df = spark.createDataFrame(pdf, schema=schema)
    df.printSchema()
    df.show()
