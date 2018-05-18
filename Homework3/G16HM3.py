'''
Homework 3: k-center vs k-means++
- develop and efficient sequential implementation of the Farthest-First Traversal algorithm for the k-center problem
- check whether k-means++, which provides a good initialization for the Lloyd's algorithm, can be executed on a
  coreset extracted through k-center, rather than on the whole dataset, without sacrificing the quality of its
  output too much
'''

import sys
import time

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns


# get file path from argv or asks it from input if missing
def fetch_file(has_argv):
    if has_argv:
        try:
            file_path = str(sys.argv[1])
        except IndexError:
            file_path = input("input file path: ")
    else:
        file_path = input("input file path: ")

    return file_path


# load dataset into numpy array
def readVectorSeq(file_path):
    try:
        dataset = np.loadtxt(file_path)
    except IOError:
        print("{} Not Found".format(file_path))
        file_path = fetch_file(False)
        return readVectorSeq(file_path)

    return dataset


# distance between 2 points
def distance(p1, p2):
    return np.linalg.norm(p1 - p2)


# kcenter(Dataset, k)
# Dataset = dataset
# k = number of centers
# return the set of Centers
def kcenter(Dataset, k):
    # control on centers
    while k < 2:
        print("kcenter requires a number of centers >1")
        k = input("input new number of centers:")

    # pick arbitrary center from the point set and remove it from Dataset
    Centers = np.array([Dataset[0]])
    Dataset = np.delete(Dataset, 0, 0)

    # initializes array of current distances from every point to every center
    distances = np.zeros(len(Dataset))

    for _ in range(2, k + 1):
        # computes distances from the last found center
        for i, p in enumerate(Dataset):
            distances[i] += distance(Centers[-1], p)

        # finds new center
        max_d = 0
        for i in range(len(Dataset)):
            if distances[i] > max_d:
                max_d = distances[i]
                point = i

        # add new found center and remove it from the point set
        Centers = np.append(Centers, [Dataset[point]], axis=0)
        Dataset = np.delete(Dataset, point, 0)
        distances = np.delete(distances, point, 0)

    return Centers


# A function kmeansPP(P,k) that receives in input a set of points P and an integer k, and returns a set C of k centers
# computed with the kmeans++ algorithm.
def kmeansPP(Dataset, WP, k):
    # select initial center with uniform probability from the point set
    i = np.random.randint(0, len(Dataset))
    Centers = np.array([Dataset[i]])
    Dataset = np.delete(Dataset, i, 0)

    if WP is not None:
        WP = np.delete(WP, i, 0)

    # initializes array of current distances from every point to every center
    min_distances = np.full(len(Dataset), np.inf)

    # initializes array of probabilities for center selection
    center_probs = np.zeros(len(Dataset))

    for _ in range(2, k + 1):
        # computes distances from the last found center and keeps the smaller one
        for i, p in enumerate(Dataset):
            d = distance(Centers[-1], p)
            if d < min_distances[i]:
                min_distances[i] = d

        # selects new center
        if WP is not None:
            center_probs = (WP * min_distances) ** 2 / np.sum((WP * min_distances) ** 2)
        else:
            center_probs = min_distances ** 2 / np.sum(min_distances ** 2)

        # picks 1 from the interval 0..len(Dataset)-1 with probability given by the distribution computed above
        point = np.random.choice(len(Dataset), 1, p=center_probs)[0]

        # add new found center and remove it from the point set
        Centers = np.append(Centers, [Dataset[point]], axis=0)
        Dataset = np.delete(Dataset, point, 0)
        min_distances = np.delete(min_distances, point, 0)

        if WP is not None:
            WP = np.delete(WP, point, 0)

    return Centers


# kmeansObj(Dataset, Centers)
# Dataset = Dataset
# Centers = List of Centers
# returns the average squared distance of a point of Dataset from its closest center
# A function kmeansObj(P,C) that receives in input a set of points P and a set of centers C,
# and returns the average squared distance of a point of P from its closest center
# (i.e., the kmeans objective function for P with centers C, divided by the number of points of P).
def kmeansObj(Dataset, Centers):
    #inizialize array
    min_distances = np.full(len(Dataset), np.inf)

    # creates array of minumum distances from every point to the set of centers
    for i, p in enumerate(Dataset):
        for c in Centers:
            dist = distance(p, c)
            if dist < min_distances[i]:
                min_distances[i] = dist

    return sum(min_distances ** 2) / len(Dataset)


# main
def main():
    # fetch dataset
    file_path = fetch_file(True)
    dataset = readVectorSeq(file_path)

    # weights
    weights = np.ones(len(dataset))

    # input number of centers
    k = int(input("Input number of centers k>1: "))
    
    # kcenter
    print("Start kcenter")
    begin = time.time()
    kcenter(dataset, k)
    print('execution:', time.time() - begin, 'sec')
    print("\n")

    # kmeansPP
    print("Start kmeansPP")
    begin = time.time()
    centers = kmeansPP(dataset, weights, k)
    print('execution:', time.time() - begin, 'sec')
    print("\n")

    # kmeansObj
    print("Start kmeansObj\nP = entire dataset\nC = centers calculated with kmeansPP on P")
    begin = time.time()
    print("Average Squared Distance: {}".format(kmeansObj(dataset,centers)))
    print('execution:', time.time() - begin, 'sec')
    print("\n")

    # kcenter->kmeansPP->kmeansObj
    k1 = 0
    while k1<k:
        k1 = int(input("Input number of centers k1>k: "))
        if k1<k:
            print("Invalid k1 value")

    print("Start kmeansObj\nP = entire dataset\nC = k centers calculated with kmeansPP in X\n"+
          "X = k1 centers calculated with kcenter on P")
    X = kcenter(dataset,k1)
    weights = np.ones(len(X))
    centers = kmeansPP(X, weights, k)
    begin = time.time()
    print("Average Squared Distance: {}".format(kmeansObj(dataset,centers)))
    print('execution:', time.time() - begin, 'sec')
    print("\n")


if __name__ == "__main__":
    main()
