'''
MR-samplesort
'''
import numpy as np
from pyspark import SparkConf, SparkContext


def g(item,k):
    
    return (item[0]%k,(0,item[1]))

# Round1
def round1(dataset,k):
    return dataset.map(lambda x: g(x,k))


# Round2
def round2(dataset):
    return dataset


# Round3
def round3(dataset):
    return dataset


# main
def main():
    sparkConf = SparkConf(True).setAppName("samplesort").setMaster("local[*]")
    sc = SparkContext.getOrCreate(sparkConf)
    arraydim = int(input("array length: "))
    k = int(input("splitters: "))
    rand = np.random.rand(arraydim)
    arr = []
    for i in range(0, len(rand)):
        arr.append((i, rand[i]))
    dataset = sc.parallelize(arr)
    print("INPUT")
    print(dataset.collect())

    # Round1
    dataset = round1(dataset,k)
    print("AFTER ROUND 1")
    print(dataset.collect())

    # # Round2
    # dataset = round2(dataset)
    # print("AFTER ROUND 2")
    # print(dataset.collect())

    # # Round3
    # dataset = round3(dataset)
    # print("AFTER ROUND 3")
    # print(dataset.collect())

    return


if __name__ == "__main__":
    main()
