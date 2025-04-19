# <b>PySpark Introduction</b>

Spark is an Open Source Unified Computing Engine with set of libraries for <b>parallel data processing</b> on Computer Cluster.

It supports widely used Programming languages such as:
1. Python
2. Java
3. Scala
4. R
> It processes data in memory (RAM) which makes it <b>100 times faster</b> than traditional Hadoop Map Reduce.

## Spark Components

1. Low Level API - RDD & Distributed Variables
2. Structured API - DataFrames, Datasets and SQL
3. Libraries and Ecosystem, Structured Streaming and Advanced Analytics

## How Spark Works? 
1. Driver
*  Heart of Spark Application
* Manages the information and state of executors
* Analyzes, distributes and Schedule the work on executors
2. Executor
* Execute the code
* Report to Driver with Execution status

> <b>Partition</b> - For the parallel working of a task Spark breaks down the data into chunks called Partitions.

> <b>Transformation</b> - The instruction or code to modify and transform data to find some useful information.
Eg. `select` ,`where` ,`groupby`, etc.

> <b>Actions</b> - To trigger the execution we need to call an Action.<br> Types: <br> - View data in console <br>- Collect data to native languages <br> - Write data to output data sources

## Spark prefers Lazy Evaluation
Spark waits till the last moment to execute the graph of computation.<br> This allows it ot optimize, plain and use the resouces properly for execution.

## Spark Session
1. The Driver process is known as Spark Session.
2. It is the entry point for Spark execution.
3. The Spark Session instance executes the code in the cluster.
