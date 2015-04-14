## Hadoop vs Relational

The goal of this project is to serve as a tutorial of Hadoop programming with a more realistic scenario going above the traditional "word count" example.

In this project, we solve a real world problem "What are the most polluted cities in Canada" by using the public data from the [National Air Pollution Surveillance Program](http://maps-cartes.ec.gc.ca/rnspa-naps/data.aspx?lang=en). There are 4 different solutions. Each is implemented using the native "best practices" (not a simple syntax port).

- Relational using SQL Server 2012R2 (SSIS + T-SQL)
- Hadoop, using Java MapReduce
- Hadoop, using Pig
- Hadoop, using Hive + custom Java UDF

## Benchmark results 

- Parsing a fixed-length file of 10BM, 74065 records
- Join StationID to Station MetaData to Substitute StationID by Province, CityName
- Group (Province, CityName), Max(Average Ozone in 2012)
- Sort Grouping Result by descending values of Ozone
<br/><br/>

### Hadoop single node cluster
Hortonworks Sandbox HDP 2.1 (Hadoop 2.40)

|                |SQL2012 R2       |   Hive          |      Pig    |  Java MapReduce |
|----------------|-----------------|-----------------|-------------|-----------------|
|Exec Time       |  6 secs         |   138 secs      |    194 secs |     70 secs     |
|Machine         | Elitebook 8530w<br/>P8700 @ 2.5GHz, 8GB | VM, 6GB<br/>HyperV 2012R2<br/>AMD FX-8320<br/>32GB physical| same as Hive|  same as Hive   |


### Hadoop 4 nodes cluster
Hortonworks HDP 2.1.2 (Hadoop 2.40)

|                |SQL2012 R2       | Hive (default) |  Hive TEZ|      Pig    |  Java MapReduce |
|----------------|-----------------|---------|--------|-------------|-----------------|
|Exec Time       |  6 secs         | 144 secs|  61 secs |    205 secs |     69 secs     |
|Machine         | Elitebook 8530w<br/>P8700 @ 2.5GHz, 8GB | 4x VM, 6GB<br/>(CentOS 6.5, 1 CPU)<br/>HyperV 2012R2<br/>AMD FX-8320<br/>32GB physical| same as Hive|  same as Hive   | same as Hive |

## Thank You
Detailed [Project Description](./AirQualityAnalysis/AirQualityAnalysis_Readme.txt)

This tutorial was presented in:

- Toronto SQL Server User Group (April 2014)
- Toronto Hadoop User Group (May 2014)

<a href="mailto:tritanix@gmail.com?Subject=GitHub%20HadoopLab%20AirQualityAnalysis" target="_top">Tri Nguyen</a><br/>
<br/>
