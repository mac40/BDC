import sys
import os.path
import time
from pyspark import SparkContext, SparkConf

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def main():
    # Get file path from command line arguments, print error message if no path is given
    # or the input file doesn't exist
    try:
        file_path = str(sys.argv[1])
        if not os.path.isfile(file_path):
            sys.exit()
    except:
        print("Invalid Input")
        print("Check if path and file name are correct")
        sys.exit()

    ## Create a spark context sc
    sparkConf = SparkConf(True).setAppName("Word_Count").setMaster("local[*]")
    sc = SparkContext.getOrCreate(sparkConf)

    ## Save the text file into a rdd
    dDocuments = sc.textFile(file_path).cache()

    ## Count the number of documents present in the text file
    ## This force the loading of the file, necessary to measure time properly
    global nDocuments ## This is a global variable we will use in version 2 of Word Count
    nDocuments = dDocuments.count()
    print("\nThere are ", nDocuments, " documents in the text file")

    ################
    ## Word Count ##
    ################

    ## First version, compute the word count for each document
    wc1 = word_count_1(dDocuments)
    print("\nTime required by Word Count v1: {:8.5f} s".format(wc1[0]))

    ## Second version, compute the word count in two rounds
    wc2 = word_count_2(dDocuments)
    print("\nTime required by Word Count v2: {:8.5f} s".format(wc2[0]))

    ## This version of word count use the method reduceByKey()
    ## wc3 is a tuple (time, word_count)
    ## we will use this to get the k most frequent words
    wc3 = word_count_3(dDocuments)
    print("\nTime required by Word Count v3: {:8.5f} s".format(wc3[0]))

    ##################################
    ## Return k most frequent words ##
    ##################################
    k = input("\nInsert the number of most frequent words to be returned: ")
    while not k.isnumeric():
        k = input("Please, insert a valid input (an integer number): ")
    print("The",  k, "most frequent words are: ")
    top_k(wc3[1],int(k))

    ## Run the three word count methods a few times to compare their performances
    ## the resulting plot is stored in Benchmark.pdf
    ## Here, just for comparison, we consider also a word count made without spark
    bench = input("\nEnter y/n to run a benchmark/exit: ")
    if bench == "y":
        benchmark(dDocuments, file_path)

    print("\nCreated a file \'Benchmark.pdf\' in the current directory" )
    end = input("\nPress enter to exit...\n")

###########
###########

################
## Word count 1
def word_count_1(dDoc):

    wordCount = dDoc.flatMap(lambda doc: document_count(doc))\
                .groupByKey()\
                .map(lambda it: (it[0],sum([c for c in it[1]])))\

    start = time.time()
    _ = wordCount.count()
    stop = time.time()
    ## Return a tuple (time, wordCount)
    return  (stop-start, wordCount)

def document_count(document):
    words = {}
    for w in document.strip().split():
        words[w] = words.setdefault(w, 0)+1
    return list(words.items()) #Return a list of tuples (w, c_i(w))

################
## Word count 2
nDocuments = 0 ## Define it as a global variable, we will need it on the map function of round 1

def word_count_2(dDoc):

    ## Round 1 map phase: produce touple (x, (w, c_i(w)))
    ##         reduce   : Produce (w, c(x,w))

    ## Round 2 map phase: Identity
    ##         reduce   : gather (w, c(x,w)) and produce (w, c(w))
    wordCount = dDoc.flatMap(lambda doc: wc2_r1_map(doc))\
                    .groupByKey()\
                    .flatMap(lambda it: wc2_r1_reduce(it))\
                    .groupByKey()\
                    .map(lambda it: (it[0],sum([c for c in it[1]]))).cache()
    start = time.time()
    _ = wordCount.count()
    stop = time.time()
    ## Return a tuple (time, wordCount)
    return  (stop-start, wordCount)

def wc2_r1_map(doc): ## WC2 map function for round 1
    words = {}
    for w in doc.strip().split():
        words[w] = words.setdefault(w, 0)+1
    x = np.random.randint(np.sqrt(nDocuments)) ## Generate a random mumber beteween [0,sqrt(N))
    return [(x, t) for t in words.items()]

def wc2_r1_reduce(t): ## WC2 reduce function for round 1
    t_it = t[1] # Contains (w, c_i(w))
    words = {}
    for p in t_it:
        w = p[0]
        c_i = p[1]
        words[w] = words.setdefault(w, 0)+c_i
    return list(words.items())


################
## Word count 3

def word_count_3(dDoc):

    wordCount = dDoc.flatMap(lambda doc: doc.strip().split(" "))\
                    .map(lambda w: (w,1))\
                    .reduceByKey(lambda w1, w2: w1+w2).cache()

    start = time.time()
    _ = wordCount.count()
    stop = time.time()
    ## Return a tuple (time, wordCount)
    return  (stop-start, wordCount)

## Print the k most frequent words (set k default value to 10)
def top_k(wordCount, k=10):
    topk = wordCount.takeOrdered(k, key = lambda x: -x[1])
    for i in topk:
        print(i)

def benchmark(dDoc, file_path):
    lVersion = []
    lTime = []
    nBenchmark = 5
    for i in range(1, nBenchmark):
        print("Benchmark {}/{}".format(i, nBenchmark))
        lVersion.append(1)
        lTime.append(word_count_1(dDoc)[0])

        lVersion.append(2)
        lTime.append(word_count_2(dDoc)[0])

        lVersion.append(3)
        lTime.append(word_count_3(dDoc)[0])

    ## For semplicity, convert the 2 list into a DataFrame
    df = pd.DataFrame({"Version": lVersion, "Time":lTime})
    df.head()
    ax = sns.barplot(df["Version"], df["Time"])
    ax.set_title('Benchmark')
    ax.set_xlabel("Version")
    ax.set_ylabel("Time (s)")
    plt.savefig("Benchmark.pdf")

if __name__=='__main__':
    main()
