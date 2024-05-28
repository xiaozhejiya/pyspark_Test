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
    rdd = sc.parallelize([
        ('张三', 'class_1', 99),
        ('王五', 'class_2', 35),
        ('王二', 'class_3', 57),
        ('王久', 'class_4', 12),
        ('王丽', 'class_5', 99),
        ('王娟', 'class_1', 90),
        ('王军', 'class_2', 91),
        ('王俊', 'class_3', 33),
        ('王君', 'class_4', 55),
        ('王琦', 'class_5', 66),
        ('郑颖', 'class_1', 11),
        ('郑辉', 'class_2', 33),
        ('张丽', 'class_3', 36),
        ('张张', 'class_4', 79),
        ('黄帆', 'class_5', 90),
        ('黄凯', 'class_1', 90),
        ('黄恒', 'class_2', 11),
        ('王凯', 'class_3', 11),
        ('王凯杰', 'class_1', 11),
        ('王开杰', 'class_2', 33),
        ('王系亮', 'class_3', 99),
    ])
    schema = StructType().add("name", StringType()).add("class", StringType()).add("score", IntegerType())
    df = rdd.toDF(schema=schema)
    df.createTempView("stu")

    # 窗口函数
    spark.sql("""
        select *, AVG(score) over() as avg_score from stu
    """).show()

    # 窗口排序函数
    spark.sql("""
        select *, ROW_NUMBER() over(order by score desc) as row_number,
        DENSE_RANK() OVER(PARTITION BY class ORDER BY score) as dense_rank,
        RANK() over(order by score) as rank
        from stu
    """).show()

    # NTILE
    spark.sql("""
        select *, NTILE(6) over(order by score desc) from stu
    """).show()
