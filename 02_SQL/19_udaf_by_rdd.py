from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
from pyspark.sql import functions as F
import os

os.environ["HADOOP_CONF_DIR"] = "/export/server/hadoop-3.3.0/etc/hadoop"
if __name__ == '__main__':
    # 构建执行环境入口对象
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize(range(1, 6), 3)
    df = rdd.map(lambda x:[x]).toDF(["num"])
    # 使用mapPartitions算子来完成聚合操作
    rdd = df.rdd.repartition(1)

    def process(iter):
        sum = 0
        for row in iter:
            sum += row["num"]

        return [sum]
    print(rdd.mapPartitions(process).collect())