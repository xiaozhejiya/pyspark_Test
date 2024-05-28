from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
from pyspark.sql import functions as F
import os

os.environ["HADOOP_CONF_DIR"] = "/export/server/hadoop-3.3.0/etc/hadoop"
if __name__ == '__main__':
    # 构建执行环境入口对象
    # 添加配置文件
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        config("spark.sql.warehouse.dir", "hdfs://node5:8020/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://node5:9083"). \
        enableHiveSupport().\
        getOrCreate()
    sc = spark.sparkContext
    spark.sql("show databases").show()
