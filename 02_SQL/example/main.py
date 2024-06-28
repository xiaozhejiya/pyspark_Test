from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import pyspark
from pyspark.sql.types import StringType
from pyspark.storagelevel import StorageLevel

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
        master("local[*]"). \
        config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://node1:9083"). \
        enableHiveSupport(). \
        getOrCreate()
    # 读取数据
    # 省份存在缺失值,因此需要缺失值处理
    # 省份还存在是Null的数据
    # money字段存在数额太大的测试数据
    # 进行列值裁剪
    df: pyspark.sql.dataframe.DataFrame = spark.read.format("json").load("../../data/test_data/mini.json"). \
        dropna(thresh=1, subset=["storeProvince"]). \
        filter("storeProvince != 'null'"). \
        filter("receivable < 10000"). \
        select("storeProvince", "storeID", "receivable", "dateTS", "payType")
    # TODO:1.各省份销售额统计
    # 保存两位小数
    province_sale_df = df.groupBy("storeProvince").sum("receivable"). \
        withColumnRenamed("sum(receivable)", "money"). \
        withColumn("money", F.round("money", 2)). \
        orderBy("money", ascending=False)
    province_sale_df.show(truncate=False)

    # 写出到mysql
    # 如果存在中文字符,则需要设置

    """
    -- 查看数据库字符集
    SHOW CREATE DATABASE your_database_name;

    -- 查看表的字符集
    SHOW CREATE TABLE your_table_name;
    -- 修改字符集
    ALTER DATABASE bigdata CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

    """

    province_sale_df.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://127.0.0.1:3306/bigdata?useSSL=false&useUnicode=true&characterEncoding=UTF-8"). \
        option("dbtable", "province_sale"). \
        option("user", "root"). \
        option("password", "hadoop"). \
        option("encoding", "utf-8"). \
        save()
    # 写除到hive
    province_sale_df.write.mode("overwrite").saveAsTable("default.province_sale", "parquet")

    print(type(df))
    # TODO:2,top3销售身份中日销售达到1000+,的店铺数量
    # province_top3 = df.groupBy("storeProvince"). \
    #     sum("receivable"). \
    #     withColumnRenamed("sum(receivable)", "money"). \
    #     withColumn("money", F.round("money", 2)). \
    #     orderBy("money", ascending=False). \
    #     select("storeProvince"). \
    #     take(3)
    # value_list = [i.asDict()["storeProvince"] for i in province_top3]
    # print(value_list)
    #
    #
    # def filter_pro(pro):
    #     if pro in value_list:
    #         return pro
    #
    #
    # udf1 = F.udf(filter_pro, StringType())
    # df.select(udf1(df["storeProvince"]), "receivable"). \
    #     filter(df["receivable"] > 1000). \
    #     orderBy("receivable", ascending=False). \
    #     show()
    # 获取前三的省
    top3_province_df = province_sale_df.limit(3).select("storeProvince").withColumnRenamed("storeProvince",
                                                                                           "top3_storeProvince")
    # 原始数据和前三表join
    top3_province_df_joined = df.join(top3_province_df,
                                      on=df["storeProvince"] == top3_province_df["top3_storeProvince"])
    # 添加到内存
    top3_province_df_joined.persist(StorageLevel.MEMORY_AND_DISK)
    province_hot_store_count_df = top3_province_df_joined.groupBy("storeProvince", "storeID",
                                                                  F.from_unixtime(df["dateTS"].substr(0, 10),
                                                                                  "yyyy-MM-dd").alias("day")). \
        sum("receivable").withColumnRenamed("sum(receivable)", "money"). \
        filter("money > 1000"). \
        dropDuplicates(subset=["storeID"]). \
        groupBy("storeProvince").count()
    province_hot_store_count_df.show()
    province_hot_store_count_df.write.mode("overwrite"). \
        format("jdbc"). \
        option("url", "jdbc:mysql://127.0.0.1:3306/bigdata?useSSL=false&useUnicode=true&characterEncoding=UTF-8"). \
        option("dbtable", "province_hot_store_count_df_mysql"). \
        option("user", "root"). \
        option("password", "hadoop"). \
        option("encoding", "utf-8"). \
        save()
    # 写入到hive
    province_hot_store_count_df.write.mode("overwrite").saveAsTable("bigdata.province_hot_store_count_df_hive",
                                                                    "parquet")
    # 从内存删除
    top3_province_df_joined.unpersist()


