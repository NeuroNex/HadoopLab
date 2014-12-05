=======================================
Ozone Analysis using SQL Server 2012R2 or SQLServer 2014

2014-06-01 - Tri Nguyen
2014-06-07 - Tested working 100% with SQLServer2014, code unchanged
=======================================

OVERVIEW
=========
This is the solution described as "SQL2012 R2" which achieves the Ozone Analysis results in 6 seconds.

The details of the scenario to solve is described in:
/home/tri/Documents/HadoopLab/AirQualityAnalysis/AirQualityAnalysis_Readme.txt

The Benchmark results comparing the SQLServer Solution vs Hadoop tools
(Pig, Hive, Java MapReduce) is described in:
/home/tri/Documents/HadoopLab/AirQualityAnalysis/Results/BenchmarkResults.txt


THE SQLServer PROJECT
=====================
The OzoneAnalysis_bySQLServer.7z file represents the entire SQLServer Project.
It is archived as compressed because the detailed files are not usable by IntelliJ.

There are two projects:

- The SSIS package (the ETL tool, named SQL Server Integration Service)
  DevTool:
  - Visual Studio 2010 (SQLServer 2012R2)
  - Visual Studio 2012 (SQLServer 2014)

- The SQL Scripts
  DevTool: SQL Server Management Studio

To run these project, you would need:
- SQL Server 2012R2 or SQLServer 2014
  . Data Engine
  . Integration Service

===(end)===

