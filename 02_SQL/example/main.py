from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
"""
需求1:各省销售额的统计
需求2:top3销售省份中,有多少店铺达到过日销售1000+
需求3:top3省份中,各省的平均单价
需求4:top3身份中,各省份的支付类型比例
"""
os.environ["HADOOP_CONF_DIR"] = "/export/server.hadoop-3.3.0/etv/hadoop"
if __name__ == '__main__':
    # 创建连接
    spark = SparkSession.builder.appName("SparkSQL_Examplr"). \
        master("yarn").\
        config("spark.sql.warehouse.dir", "hdfs://node5:8020/user/hive/warehouse").\
        config("hive.metastore.uris", "thrift://node3:9083").\
        enableHiveSupport().\
        getOrCreate()
    # 读取数据
    # 省份存在缺失值,因此需要缺失值处理
    # 省份还存在是Null的数据
    # money字段存在数额太大的测试数据
    # 进行列值裁剪
    df = spark.read.format("json").load("../data/test_data/mini.json").\
        dropna(thresh=1, subset=["storeProvince"]).\
        filter("storeProvince != 'null'").\
        filter("money < 10000").\
        select("storeProvince", "storeID", "receivable", "dataTS", "payType")
    # TODO:1.各省份销售额统计
    # 保存两位小数
    province_sale_df = df.groupBy("storeProvince").sum("receivable").\
        withColumnRenamed("sum(receivable)", "money").\
        withColumn("money", F.round("money", 2)).\
        orderBy("money", ascending=False)
    province_sale_df.show(truncate=False)


