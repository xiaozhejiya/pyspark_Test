from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    # 通过SparkSession对象获取sparkContext
    sc = spark.sparkContext

    # sparkSQL的hello word
    df = spark.read.csv("../data/test_data/stu_score.txt", sep=",", header=False)
    df2 = df.toDF("id", "name", "score")
    df2.printSchema()
    df2.show()

    df2.createTempView("score")
    # sql风格
    spark.sql("""
        select * from score where id ='1';
        """).show()
    # dsl风格
    df2.where("id='1'").show()