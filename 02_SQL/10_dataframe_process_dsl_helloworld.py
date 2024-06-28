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
        schema("id INT, subject STRING, score INT"). \
        load("../data/test_data/sql/stu_score.txt")

    # column对象获取
    id_column = df["id"]
    subject_column = df["subject"]

    # DSL风格演示
    df.select("id", "subject").limit(5).show()
    df.select(["id", "subject"]).limit(5).show()
    df.select(id_column, subject_column).limit(5).show()

    # filter API
    df.filter("score < 99").limit(5).show()
    df.filter(df["score"] < 99).limit(5).show()

    # where API
    df.where("score < 99").limit(5).show()
    df.where(df["score"] < 99).limit(5).show()

    # 分组聚合
    # :groupBy返回值类型为GroupedData: 不是dataFrame,但是可以使用聚合方法
    df.groupBy("subject").count().show()
