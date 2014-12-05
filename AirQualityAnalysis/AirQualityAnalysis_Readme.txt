==================================================
Toronto User Group Session of April 2012
Toronto Hadoop User Group of May 2014

Discovery of Hadoop Under a Relational Lens Scope

2014-05-30 - Tritanix@gmail.com
==================================================

1. THE PROBLEM TO SOLVE
=======================
The scenario is to parse a fixed legnth file containing Ozone measures for 2012
all around Canada and produce a report of "Most polluted cities". Example:

ONTARIO	ALGOMA	82
ONTARIO	SIMCOE	74
ONTARIO	GRAND BEND	73
ONTARIO	HAMILTON	69
QUEBEC	ROUYN-NORANDA	69
...etc..

The files are available at
National Air Pollution Surveillance Program (NAPS)
http://maps-cartes.ec.gc.ca/rnspa-naps/data.aspx?lang=en

All files needed for this project are in the project folder src/main/resources
Check the 00-Readme.txt file for more details


2. THE 4 POSSIBLE SOLUTIONS
===========================

The idea of this project is to show how to achieve a scenario using 4 solutions:
SQL Server 2012R2 (also tested OK with SQL Server 2014), Hive, Pig and Java MapReduce.
A conscencious effort has been made to use the "best practices" corresponding to each tool. Which means:

- The Hive implementation solves the solution the "Hive" way.
  It is not an attempt to make it look like SQL and solve the SQL way

- The Pig implentation uses Pig's own feature to solve the solution
  For the scenario at hand, the Pig implementation is actually the most elegant

- Java MapReduce uses the latest API (Hadoop 2.40 as of this writing)
  NOTE: the MR Job is not the best design, there are ways to improve it:
  . Better way to chaine 2 jobs
  . Better way to join datasets
  . Currently I made a lazy implementation in the reducer which outputs (OzoneValue, Location).
    This must be (Location, OzoneValue) to make it truly comparable to Pig & Hive results.
  
  The reader is encouraged to implement these improvements

- The SQLServer solution (also supplied in the IntelliJ project)
  Of course also uses the best of breed tool SQLServer 2012R2 / 2014:
  . SQLServer Integration Service package 
  . T-SQL scripts (hope that you will appreciate the geekness of these scripts)


3. DEV ENVIRONMENT
==================
- OS: Xubuntu 14.04 64 bits
- Java: OpenJDK 1.7.0_55 64 bits
- IntelliJ IDEA v13.1.3 Community Edition v13.1.3 : http://www.jetbrains.com/idea/
- Git 1.9.1
- Hadoop 2.40: http://mirror.csclub.uwaterloo.ca/apache/hadoop/common/hadoop-2.4.0/hadoop-2.4.0.tar.gz
- Maven 3.2.1: http://maven.apache.org/download.cgi


- Hadoop client node:
  this is where the Java MapReduce code, Pig and Hive scripts will be deployed and executed
  The Dev machine doesn't have Hadoop and cannot execute any of these codes.

  We'll use a "Pseudo Distributed Cluster" which is Hortonworks HDP Sandbox 2.1
  http://hortonworks.com/products/hortonworks-sandbox/

  NOTE: you must have the HDP Sandbox running using a static IP
  The code in this project assumes that the HDP Sandbox name and IP is:
  192.168.56.102   HDPSBHV # HDP Sandbox

  It is possible to use any name and IP. You just need to change the deployement statements accordingly
  Each code and script in this project is supplied with deployment instructions in the comment section

- Hadoop component versions (within the HDP Sandbox 2.1)
  . Hadoop 2.40
  . Pig 0.12.1
  . Hive 0.13.0


4. TEST RESULTS
===============
Read /home/tri/Documents/HadoopLab/AirQualityAnalysis/Results/BenchmarkResults.txt

5. PROJECT FOLDERS
===================

AirQualityAnalysis:
-------------------
- Results       : contains the final results of each solution
- Screenshots   : showing various stages of the 4 solutions (Hive, Pig, Java MR, SCQServer)
- src/main/hive : the HiveQL scripts
- src/main/pig  : the Pig script

- src/main/java : various R&D in Java MapReduce to arrive to the solution
                  all java code use Hadoop 2.40 API (no deprecated code from old API)

  . SimpleDriver, SimpleStationMapper, SimpleMaxAvgReducer
    The simplest Java MR, mainly to test Hadoop and data input file
    This MR program is not sufficiant to provide the solution comparable to the 3 other solutions

  . LocationDriver, LocationMapper, SimpleMaxAvgReducer
    Improve the "Simple" Java MR with two techniques a map-side join and a distributed cache
    to substitute StationID by LocationName

  . OzoneAnalysisDriver
    this is the Java MR which provides the final solution

- src/main/resources : data files used as input for all solutions

- src/test/java : JUnit Tests for the java solution

OzoneAnalysis_using_SQLServer
-----------------------------
The main focus of this project is the Hadoop solution. The SQLServer solution is not runable or editable
under Hadoop Dev environment. The SQLServer solution is supplied here in compressed format as courtesy in case you are interested.  

You will need to uncompress and use a dev environment of SQLServer 2012R2 or SQLServer 2014
to open review and run the solution. 


6. COPYRIGHGTS
==============
The code is free and can be used for private or commercial purposes.
You don't need to ask for permission. However the author would appreciate
if you mention the author name and the URL where you get the code.

In the spirit of knowledge sharing. If you have made any significant improvement to the code,
the author would appreciate if you can contribute your changes to the benefits of all.

Thank you and enjoy Hadoop Programming.

Tri Nguyen, tritanix@gmail.com

===== (end) =====
