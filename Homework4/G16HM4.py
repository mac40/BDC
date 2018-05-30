'''
Homework 4:
- A function runMapReduce(pointsrdd,k,numBlocks) that receives in input a set of points
  represented by an RDD pointsrdd of Vector and two integers k and numBlocks, and does the following
  things:
    (a) partitions pointsrdd into numBlocks subsets;
    (b) extracts k points from each subset by running the sequential Farthest-First
        Traversal algorithm implemented for Homework 3;
    (c) gathers the numBlocks*k points extracted into a list of Vector coreset;
    (d) returns a a list of Vector with the k points determined by running the sequential
        max-diversity algorithm with input coreset and k.
- A function measure(pointslist) that receives in input a set of points represented as a list
  pointslist and returns a double which is the average distance between all points in pointslist
  (i.e., the sum of all pairwise distances divided by the number of distinct pairs).
'''


import sys
import time

import numpy as np
from pyspark import SparkConf, SparkContext
from pyspark.mllib.linalg import Vectors


# points is a list of Vectors , k an integer
def runSequential(points, k):

    n = len(points)
    if k >= n:
        return points

    result = list()
    candidates = np.full(n, True)

    for _ in range(int(k / 2)):
        maxDist = 0.0
        maxI = 0
        maxJ = 0
        for i in range(n):
            if candidates[i] == True:
                for j in range(n):
                    d = Vectors.squared_distance(points[i], points[j])
                    if d > maxDist:
                        maxDist = d
                        maxI = i
                        maxJ = j
        result.append(points[maxI])
        result.append(points[maxJ])
        #print "selecting "+str(maxI)+" and "+str(maxJ)
        candidates[maxI] = False
        candidates[maxJ] = False

    if k % 2 != 0:
        s = np.random.randint(n)
        for i in range(n):
            j = (i + s) % n
            if candidates[j] == True:
                #print "selecting "+str(j)
                result.append(points[i])
                break

    return result


# kcenter(P, k)
# P = dataset
# k = number of centers
# return the set of Centers
def kcenter(P, k):
    points = np.copy(P)
    centers = np.zeros((k, P.shape[1]))

    np.random.seed(42)
    idx = np.random.randint(P.shape[0])
    centers[0] = points[idx]

    ## 0 if the point is not a center
    distances = [[Vectors.squared_distance(centers[0], p), 0] for p in points]
    distances[idx][1] = 1

    ## distances is a list [dist, tag] where tag=1 if the point is a center

    for c in range(1, k):
        ## Find the distance of the closest center
        for i, p in enumerate(points):
            if distances[i][1] == 1:
                continue
            d_tmp = Vectors.squared_distance(centers[c-1], p)
            if d_tmp > 0 and d_tmp < distances[i][0]:
                distances[i][0] = d_tmp

        ## find the max
        max_c = 0
        c_idx = 0
        for i, (d, tag) in enumerate(distances):
            if d > max_c and tag == 0:
                max_c = d
                c_idx = i
        centers[c] = points[c_idx]
        distances[c_idx][1] = 1

    return centers


# f
# create Vector from a string
def f(line):
    return Vectors.dense([float(l) for l in line.split(" ")])


# runMapReduce
def runMapReduce(pointsrdd, k, numBlocks):
    coreset_start = time.time()
    coreset = pointsrdd.repartition(numBlocks).mapPartitions(
        lambda data: kcenter(Vectors.dense([l for l in data]), k)).collect()
    coreset_execution = time.time() - coreset_start

    finalsol_start = time.time()
    solution = runSequential(coreset, k)
    finalsol_execution = time.time() - finalsol_start

    return coreset_execution, finalsol_execution, solution


# distance
def distance(p1, p2):
    return np.sqrt(Vectors.squared_distance(p1, p2))


# measure
def measure(pointslist):
    sum_dist = 0
    for i in range(len(pointslist)):
        for j in range(len(pointslist)):
            if j > i:
                sum_dist += distance(pointslist[i], pointslist[j])
    return sum_dist*2./(len(pointslist)*(len(pointslist)-1))


# main
def main():
    # handling argvs
    try:
        datafile = str(sys.argv[1])
        k = int(sys.argv[2])
        numBlocks = int(sys.argv[3])
    except IndexError:
        print("Missing some arguments, exiting...")
        sys.exit()

    # create the spark context
    sparkConf = SparkConf(True).setAppName("Homework4").setMaster("local[*]")
    sc = SparkContext.getOrCreate(sparkConf)

    # retrieve the dataset
    inputrdd = sc.textFile(datafile).map(f).cache()

    # call to runMapReduce
    coreset_execution, finalsol_execution, pointslist = runMapReduce(inputrdd, k, numBlocks)

    avg_distance = measure(pointslist)

    print("distance: {}".format(avg_distance))
    print("coreset execution: {}".format(coreset_execution))
    print("final execution: {}".format(finalsol_execution))
    file = open("sol.txt","a")
    file.write("{},".format(avg_distance))
    file.write("{},".format(coreset_execution))
    file.write("{}\n".format(finalsol_execution))
    file.close()


    return


if __name__ == "__main__":
    main()
