from pyspark.mllib.stat import Statistics
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.mllib.classification import SVMWithSGD
from pyspark.mllib.tree import RandomForest, DecisionTree, GradientBoostedTrees
spark = SparkSession.builder. \
    appName("test"). \
    master("local[*]"). \
    getOrCreate()
sc = spark.sparkContext

rdd1 = sc.parallelize([
    (1.0, 10.0, 100.0),
    (2.0, 20.0, 200.0),
    (3.0, 30.0, 300.0)
])
# 统计分析
summary = Statistics.colStats(rdd1)
print(summary.max())