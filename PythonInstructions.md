# Pyspark installation guide

## OSX installation guide

### PySpark - Environment  setup

* **Step 1**: Make sure that java and scala are installed on your computer (PySpark doesn't work very well with Java 9, so it's better to use Java 8)

* **Step 2**: Download Spark from [here](https://spark.apache.org/downloads.html)

* **Step 3**: Move the downloaded file in a choosen directory, for example `~/Dev` and unzip it with `tar -xvf Downloads/spark-2.1.0-bin-hadoop2.7.tgz`

* **Step 4**: Now we need to set the following environments to set the Spark path and the Py4j path in the bash profile

```none
export SPARK_HOME = ~/Dev/hadoop/spark-2.1.0-bin-hadoop2.7
export PATH = $PATH:~/Dev/hadoop/spark-2.1.0-bin-hadoop2.7/bin
export PYTHONPATH = $SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH
export PATH = $SPARK_HOME/python:$PATH

```

* **Step 5**: restart the terminal application and then test if it works by typing

```none
$SPARK_HOME/bin/pyspark
```

* **Step 6**: To run a python script, for example `FirstSparkApp.py` type

~~~none
$SPARK_HOME/bin/spark-submit FirstSparkApp.py
~~~

## Windows installation Guide

1. Download and install the JDK from [here](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)

2. Via terminal install pyspark using the following command
    ```none
    pip install pyspark
    ```
3. Download and unzip winutils from [here](https://github.com/srccodes/hadoop-common-2.2.0-bin/archive/master.zip)

4. Modify your environment variables in the following way:
    * Modify the environment variable "path" and add "path_to_unzip_winutils_folder/bin"
    * Add to the environment variables a new variable called "HADOOP_HOME" and give it value "path_to_unzip_winutils_folder"

5. In your python code you will need to add the following lines of codes
    ```python
    from pyspark import SparkContext

    sc = SparkContext.getOrCreate()
    ```

## Example

```python
import numpy as np
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("PiEstimate")
sc = SparkContext(conf = conf)

n = 10000

def f(_):
    x, y = np.random.random(2)*2-1
    if x**2+y**2 <= 1:
        return 1
    else:
        return 0

count = sc.parallelize(range(1,n+1)).map(f).reduce(lambda x, y: x+y)

print("Pi is roughly {}".format(4.0 * count/n))
```