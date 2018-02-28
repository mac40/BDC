# MapReduce
## Big data challenges
* Computational complexity
    * Any processing requiring a __superlinear number of operations__ may easily turn out __unfeasible__ (_e.g. O(n<sup>3</sup>) with big datas_)
    * If input size is really huge, just __touching all data items__ is already time consuming
    * For computation-intensive algorithms, exact solutions may be too costly. __Accurancy-efficiency tradeoffs__ (_give up accurancy for better efficiency_)
* Effective use of parallel/distributed platforms:
    * Sepcialized high-performance architectures are costly and become rapidly obsolete
    * Fault-tollerance becomes serius issue: __low Mean-Time Between Failures__ (_MTBF_)
    * Parallel/distributed programming requires __high skills__
## MapReduce
* Introduced by Google 2004
* Programming framework for handling big data
* Employed in __many application scenarios__ on __clusters of commodity processors__ and __cloud infrastructures__
* Main features:
    * Data centric view
    * Inspired by functional programming (_map, reduce functions_)
    * Ease of programming. Messy details are hidden to the programmer
* Main implementation: __Apache Hadoop__
    * extremely slow
    * High inefficiency
* Hadoop ecosystem: several variant and extensions aimed at improving Hadoop's performance (_e.g. Apache Spark_)
## Typical cluster architecture
* Racks of 16-64 __compute-nodes__ connected by fast switches
* Distributed File System
    * Files divided into chunks
    * Each chunk repicated with replicas in different nodes and, possibly, in different racks
    * The distribution of the chunks of a file is represented into a master node file wichi is also replicated. A directory records where all master nodes are
## MapReduce computation
* Computation viewed as a __sequence of rounds__.
* Each round transforms a set of __key-value pairs__ into another set of __key-value pairs__ (_data centric view!_) through the following phases
    * Map phase: a user specified __map-function__ is applied separately to each input key-value pair and procudes other key-value pairs referred as __intermediate key-value pairs__
    * Reduce phase: the __intermediate key-value pairs__ are __grouped by key__ and a user-specified __reduce function__ is applied separately to each group of key-value pairs with the same key, producing other key-value pairs which is the output of the round

## Specification of a MapReduce (MR) algorithm

1. Specify what the input and the output are
2. Make clear the sequence of rounds
3. For each round
    * input intermediate and output key-value pairs need to be clear
    * functions in the map and reduce phases need to be clear
4. Enable analysis