## Hadoop vs Relational
The goal of this project is to serve as a tutorial of Hadoop programming with a more realistic scenario going above the traditional "word count" example.

In this project, we solve a real world problem "What are the most polluted cities in Canada" by using the public data from the [National Air Pollution Surveillance Program](http://maps-cartes.ec.gc.ca/rnspa-naps/data.aspx?lang=en). There are 4 different solutions. Each is implemented using the native "best practices" (not a simple syntax port).

1. Relational using SQL Server 2012R2 (SSIS + T-SQL)
2. Hadoop, using Java MapReduce
3. Hadoop, using Pig
4. Hadoop, using Hive + custom Java UDF
5. R
6. Spark

[Project Description + Technical Details](./AirQualityAnalysis/README.md)

2014-05-30 - Tri Nguyen, released to GitHub<br/>
2015-07-12 - add solutions in R and Spark<br/>
2015-09-26 - add solution using SparkSQL, update documentation<br/>


## Benchmark Results

- Parsing a fixed-length file of 10BM, 74065 records
- Join StationID to Station MetaData to Substitute StationID by Province, CityName
- Group By (Province, CityName)
- Sort Grouping Result by descending values of Average Ozone in 2012
<br/><br/>

(Benchmark updated: **2015-07-12**)

|                |SQL2012 R2 | Hive TEZ |   Pig     | Java MR |    R     | Spark |
|                |    HW1    |    HW2   |   HW2     |   HW2   |   HW3    |       |
|----------------|-----------|----------|-----------|---------|----------|-------|
| Exec Time      |  6 secs   | 60 secs  | 3'41 secs | 70 secs |  2 secs  |  9 secs, local, HW3|
|                |           |          |           |         |          | 16 secs, HDFS on HDP, HW2  |
|----------------|-----------|----------|-----------|---------|----------|-------|
| Design Time    |  4 h      |   6 h    |   2 h     | 30 h    |  4 h     | 8 h   |

<br/><br/>

| Hardward | Description                          | OS             |
|----------|--------------------------------------|----------------|
| HW1      | Elitebook 8530w, P8700 @ 2.5GHz, 8GB | Windows 7 x64  |
| HW2      | VM (HyperV 2012R2), 8GB RAM, AMD FX-8320, 32GB physical| HDP Sandbox HDP 2.24<br>Hadoop 2.60, July 2015|
| HW3      | Desktop, Q9550 @ 2.82GHz, 8GB | Xubuntu 14.04 x64, R 3.20 | 


## Thank You
This tutorial was presented in:

- Toronto SQL Server User Group (April 2014)
- Toronto Hadoop User Group (May 2014)
- Toronto Apache Spark (Sept 2015)

<a href="mailto:tritanix@gmail.com?Subject=GitHub%20HadoopLab%20AirQualityAnalysis" target="_top">Tri Nguyen</a><br/>
<br/>
