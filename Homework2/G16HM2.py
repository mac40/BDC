'''
- Run 3 versions of MapReduce word count and returns their individual running times, carefully measured
- Ask the user to input an integer k and returns the k most frequent words, with ties broken arbitrarily
'''

import math
import random
import sys
import time

from pyspark import SparkConf, SparkContext

sparkConf = SparkConf().setAppName("Word Count").setMaster("local[4]")
sc = SparkContext(conf = sparkConf)

# Import file
try:
    file_path = str(sys.argv[1])
except IndexError:
    print("IndexError: The dataset file was not specified")
    sys.exit()

dataset = sc.textFile(file_path)

try:
    dataset.isEmpty() == False
except:
    print("Empty Dataset: The provided dataset is either non existing or empty")
    sys.exit()

# Definition of faltMap's function for Improved Word Count 1
def flatMap_func(s):
    sol = []
    for word in s:
        if word not in sol:
            sol.append(word)
    for word in sol:
        sol[sol.index(word)] = (word,s.count(word))
    return sol

# Definition of Improved Word Count 1
def IWC1(dataset):
    mapped_dataset = dataset.map(lambda s: s.split(" "))
    flatmapped_dataset = mapped_dataset.flatMap(lambda s: flatMap_func(s))
    reduced_dataset = flatmapped_dataset.reduceByKey(lambda a,b: a+b)
    return reduced_dataset

# Definition of FlatMapOne's function for Improved Word Count 2 
def flatMapOne_func(s):
    sol = []
    for word in s:
        if word not in sol:
            sol.append(word)
    for word in sol:
        sol[sol.index(word)] = (random.uniform(0,math.sqrt(len(s))),(word,s.count(word)))
        # sol[sol.index(word)] = (random.randrange(2),(word,s.count(word)))
    return sol

# Definition of Improved Word Count 2
def IWC2(dataset):
    map_one = dataset.map(lambda s: s.split(" "))
    flatmap_one = map_one.flatMap(lambda s: flatMapOne_func(s))
    for x in flatmap_one.collect():
        print(x)
    # reduced_one = flatmap_one.groupByKey()
    return dataset

# Definition of Word count w/ reduce by key
def WCRBK(dataset):
    flatmapped_dataset = dataset.flatMap(lambda s: s.split(" "))
    mapped_dataset = flatmapped_dataset.map(lambda s: (s,1))
    reduced_dataset = mapped_dataset.reduceByKey(lambda a,b:a+b)
    return reduced_dataset

# Main
start_IWC1 = time.time()
Improved_Word_Count_1 = IWC1(dataset)
_ = Improved_Word_Count_1.count()
end_IWC1 = time.time()
IWC1_time = end_IWC1 - start_IWC1
print(IWC1_time)

start_IWC2 = time.time()
Improved_Word_Count_2 = IWC2(dataset)
_ = Improved_Word_Count_2.count()
end_IWC2 = time.time()
IWC2_time = end_IWC2 - start_IWC2
print(IWC2_time)

start_WCRBK = time.time()
Word_Count_RBK = WCRBK(dataset)
_ = Word_Count_RBK.count()
end_WCRBK = time.time()
WCRBK_time = end_WCRBK - start_WCRBK
print(WCRBK_time)

# Return top k elements
k = input("Select number of elements to show:")
top_el = Word_Count_RBK.takeOrdered(k, key = lambda s: -s[1])
for x in top_el:
    print(x)