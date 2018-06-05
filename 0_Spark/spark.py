# from __future__ import print_function
# print RDD like:
# rdd.foreach(print)

from pyspark import SparkContext
from pyspark import RDD

import os

os.environ['HADOOP_HOME'] = "C:/Program Files/Hadoop"

logFile = "README.md"

sc = SparkContext.getOrCreate()
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
