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
    rdd = sc.textFile("hdfs://node5:8020/word.txt")

    rdd = rdd.flatMap(lambda x: x.split(" ")). \
        map(lambda x: [x])

    df = rdd.toDF(["word"])
    df.createTempView("words")
    spark.sql("select word, count(*) as cnt from words group by word order by cnt").show()

    # dsl风格
    df2 = spark.read.format("text").load("hdfs://node5:8020/word.txt")
    df2 = df2.withColumn("value", F.explode(F.split(df2["value"], " ")))
    df2.groupBy("value"). \
        count(). \
        withColumnRenamed("value", "word"). \
        withColumnRenamed("count", "cnt"). \
        orderBy("cnt", ascending=False). \
        show()
