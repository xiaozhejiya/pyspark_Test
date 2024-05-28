from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql import functions as F

if __name__ == '__main__':
    # 构建执行环境入口对象
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()
    sc = spark.sparkContext

    df = spark.read.format("csv"). \
        option("sep", ";"). \
        option("header", True). \
        load("../data/test_data/sql/people.csv")
    # 对全部参数进行去重
    df.dropDuplicates().show()
    # 去对age和Job字段去重
    df.dropDuplicates(["age", "job"]).show()
    # 缺失值处理
    # 无参数使用,只要有null则删除一行
    df.dropna().show()
    # 过滤掉不满足三个有效列的行
    df.dropna(thresh=3).show()
    # 过滤掉不满足指定列的行
    df.dropna(thresh=2, subset=["name", "age"]).show()

    # 丢失值填充
    # 所有丢失值都填充loss
    df.fillna("loss").show()
    # 指定丢列丢失值填充
    df.fillna("N/A", ["job"]).show()
    # 指定多列
    df.fillna({"job": "worker", "name": "位置名称", "age": -1}).show()
