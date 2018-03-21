from __future__ import print_function
from pyspark import SparkContext, SparkConf
import os
import re

os.environ['HADOOP_HOME'] = "C:/Program Files/Hadoop"
sparkConf = SparkConf().setAppName("Sum_of_Squares")
sc = SparkContext.getOrCreate()

rdd = sc.textFile("dataset_3.txt")
rdd_flatMapped = rdd.flatMap(lambda line: re.split(",| |\n|, ",line))
rdd_filtered = rdd_flatMapped.filter(lambda s: re.match(r'^\d+?\.\d+?$', s))
rdd_map = rdd_filtered.map(lambda s: float(s)**2)
rdd_reduce = rdd_map.reduce(lambda a, b: a + b)

# simple version no parser/input check
#
# rdd = sc.textFile("dataset_1.txt")
# rdd_map = rdd.map(lambda s: float(s)**2)
# rdd_reduce = rdd_map.reduce(lambda a, b: a + b)

print("Result: {}".format(rdd_reduce))
