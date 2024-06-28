# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# import numpy as np
# import pandas as pd
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,StructType
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    sc = SparkContext(master='local[*]', appName="app")
    # spark = SparkSession(sc)
    # rdd1 = sc.parallelize([
    #     (1.0, 10.0, 100.0),
    #     (2.0, 20.0, 200.0),
    #     (3.0, 30.0, 300.0)
    # ])
    rdd1 = sc.parallelize(range(10))
    print(rdd1.take(3))
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
