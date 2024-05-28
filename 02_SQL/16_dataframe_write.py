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
    df.select(F.concat_ws("---", "user_id", "movie_id", "rank", "ts")). \
        write. \
        mode("overwrite"). \
        format("text"). \
        save("../data/output/sql/text")
    df.write.mode("overwrite"). \
        option("sep", ";"). \
        option("header", True). \
        format("csv"). \
        save("../data/output/sql/csv")

    df.write.mode("overwrite").\
        format("json").\
        save("../data/output/sql/json")
    df.write.mode("overwrite").\
        format("parquet").\
        save("../data/output/sql/parquet")
