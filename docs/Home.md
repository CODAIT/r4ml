## HydraR Overview

![HydraR logo](images/hydraR-logo.png)

HydraR is a R package that allows users to use [Apache SystemML](https://sparktc.github.io/systemml/index.html) directly from the R console. HydraR opens up the world of big data to R users.

## HydraR Documentation

For more information about HydraR, please consult the following references:

- [[HydraR Intro|HydraR-Intro]]
- [HydraR GitHub README](https://github.com/SparkTC/spark-hydrar/blob/master/README.md)
- [[Installing on cluster with IBM Open Platform (IOP)|Installing-on-cluster-with-IBM-Open-Platform-(IOP)]]
- [[HydraR-A bigdata R tool on the top of SystemML and SparkR|HydraR--A-bigdata-R-tool-on-the-top-of-SystemML-and-SparkR.]]
- FAQ
  - [Examples of using HydraR](https://github.com/SparkTC/spark-hydrar/tree/master/HydraR/inst/examples)
  - [[How to connect HydraR via the client machine|How-to-connect-HydraR-via-the-client-machine]]
  - [[Migration of BigR to HydraR | Migration-of-BigR-to-HydraR]]
  - [Performance of HydraR using cache() and repartitioning()]
     - Usually, it is recommended to use the cache and repartitioning after hydrar.read.csv and after ml.preprocess() as it improves the performance
  - [A lot of Task more than 100KB warning message]
    - This is due to the fact that user didn't user hydrar.read.csv() and this is well documented 
      [here](http://stackoverflow.com/questions/33892240/large-task-size-for-simplest-program)




## Release Notes

- [[HydraR 0.8.0 Release|HydraR-Release-0.8.0]] 
