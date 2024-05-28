### spark并行度

1.配置文件中

```
conf/spark-defaults.conf中设置

spark.default.parallelism 100
```

2.在客户端中提交参数

```
bin/spark-submit --conf "spark.default.parallelism=100"
```

3.在代码中设置

```
conf = SparkConf()

conf.set("spark.default.parallelism", 100)
```

