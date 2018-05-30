'''
plot codes for final analysis
'''

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


# final execution time -> centers number
def finalex_centers(file, ax):
    mask = (file["File"] == 500000) & (file["NumBlocks"] == 25)
    temp = file.where(mask).dropna()
    plt.scatter(temp["Centers"], temp["FinalExecution"],
                color='c', axes=ax, label='Centers')


# final execution time -> partitions number
def finalex_blocks(file, ax):
    mask = (file["File"] == 500000) & (file["Centers"] == 10)
    temp = file.where(mask).dropna()
    plt.scatter(temp["NumBlocks"], temp["FinalExecution"],
                color='m', axes=ax, label='Partitions')


# final execution time -> partitions + centers numbers
def finalex_centers_blocks(file, ax):
    sub_500000 = file.where(file["File"] == 500000).dropna()
    sub_1000000 = file.where(file["File"] == 1000000).dropna()
    sub_2000000 = file.where(file["File"] == 2000000).dropna()
    sub_3000000 = file.where(file["File"] == 3000000).dropna()
    sub_6500000 = file.where(file["File"] == 6500000).dropna()

    plt.scatter(sub_500000["Centers"]*sub_500000["NumBlocks"],
                sub_500000["FinalExecution"], color='r', axes=ax, label='Centers * Partitions 500000')
    plt.scatter(sub_1000000["Centers"]*sub_1000000["NumBlocks"],
                sub_1000000["FinalExecution"], color='g', axes=ax, label='Centers * Partitions 1000000')
    plt.scatter(sub_2000000["Centers"]*sub_2000000["NumBlocks"],
                sub_2000000["FinalExecution"], color='b', axes=ax, label='Centers * Partitions 2000000')
    plt.scatter(sub_3000000["Centers"]*sub_3000000["NumBlocks"],
                sub_3000000["FinalExecution"], color='y', axes=ax, label='Centers * Partitions 3000000')
    plt.scatter(sub_6500000["Centers"]*sub_6500000["NumBlocks"],
                sub_6500000["FinalExecution"], color='k', axes=ax, label='Centers * Partitions 6500000')


# main
def main():
    file = pd.read_csv("times.csv")

    _, ax = plt.subplots(1, 1)
    finalex_centers(file, ax)
    plt.xlabel("Centers")
    plt.ylabel("RunSequential Execution Time")
    plt.legend()
    plt.show()

    _, ax1 = plt.subplots(1, 1)
    finalex_blocks(file, ax1)
    plt.xlabel("Partitions")
    plt.ylabel("RunSequential Execution Time")
    plt.legend()
    plt.show()

    _, ax2 = plt.subplots(1, 1)
    finalex_centers_blocks(file, ax2)
    plt.xlabel("Centers * Partitions")
    plt.ylabel("RunSequential Execution Time(s)")
    plt.legend()
    plt.savefig("centers_partitions_runSequential.png")
    plt.show()


if __name__ == "__main__":
    main()
