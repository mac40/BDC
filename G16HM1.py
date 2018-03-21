'''
Import dataset from txt file into an RDD structure, then use map reduce to calculate the sum of squares
'''

from pyspark import SparkContext, SparkConf
import sys
import re

# Spark initialization lines
sparkConf = SparkConf().setAppName("Sum_of_Squares")
sc = SparkContext.getOrCreate()

# Get file path from command line arguments, print error message if no path is given
try:
    file_path = str(sys.argv[1])
except:
    print("Expecting the dataset path on the command line")
    sys.exit()

# save text file into rdd, print error message if FileNotFoundError
try:
    rdd = sc.textFile(file_path)
except FileNotFoundError:
    print("File not found")
    sys.exit()

# Another way of reading data is the following one
# but it use a local list

# Read the numbers from the dataset file and
# store them in a local list lNumbers
# lNumbers = []
# with open(file_path) as file:
#     for x in file.read().split():
#         lNumbers.append(float(x))

# create parallel collection (RDD)
# dNumbers = sc.parallelize(lNumbers)

# divide elements in RDD using common separators
rdd_flatMapped = rdd.flatMap(lambda line: re.split(",| |\n|, ", line))
# filter eventual non-double/int elements in RDD
rdd_filtered = rdd_flatMapped.filter(lambda s: re.match(r'\d+\.?\d*', s))
# map phase: map every element with his square
rdd_map = rdd_filtered.map(lambda s: float(s)**2)
# reduce phase: sum all the elements
rdd_reduce = rdd_map.reduce(lambda a, b: a + b)

# simple version, input not parsed
#
# rdd = sc.textFile(file_path)
# rdd_map = rdd.map(lambda s: float(s)**2)
# rdd_reduce = rdd_map.reduce(lambda a, b: a + b)

print("Result: {}".format(rdd_reduce))
