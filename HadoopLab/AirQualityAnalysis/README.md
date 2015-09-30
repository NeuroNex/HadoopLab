1. THE PROBLEM TO SOLVE
=======================
### Source Canada Public Data:
[National Air Pollution Surveillance Program (NAPS)](http://maps-cartes.ec.gc.ca/rnspa-naps/data.aspx?lang=en)

### NAPS Stations (100K, 709 records)
```
NAPS_ID,STATION_NAME,Type,Status,TOXIC,Designated,PROVINCE,STREET_ADDRESS,CITY,COUNTRY,FSA,Postal_Code,TimeZone,Lat_Decimal,Long_Decimal,Elevation_m,SO2,CO,NO2,NO,NOX,O3,PM_10_continuous,PM_25_015_continuous,PM_25_017_dryer,PM_25_018_BAM_RH45,PM_25_019_gravimetric40/50,PM_25_020_gravimetric30/50,PM_25_021_gravimetric30,PM_25_022__FDMS,PM_1_GRIMM180,PM_25_026_GRIMM180,PM_10_GRIMM180,PM_25_030_BAM_RH3,PM_10_031_BAM_RH35,PM_25_032_SHARP,VOC,ALDEH_KETO,DICHOTX_METALS,PCB,PCDD,PBDE
10101,DUCKWORTH & ORDINANCE,C,0,,P,NEWFOUNDLAND AND LABRADOR,DUCKWORTH & ORDINANCE,ST. JOHN'S,CANADA,A1C,A1C 1E4,-3.5,47.56806,-52.70222,7,X,X,X,X,X,X,,,,,,,,,,,,,,,,,,,,
10102,WATER STREET POST OFFICE,C,1,X,Y,NEWFOUNDLAND AND LABRADOR,354 WATER STREET,ST. JOHN'S,CANADA,A1C,A1C 1C4,-3.5,47.56,-52.71139,17,X,X,X,X,X,X,,,,,,,,,,,,X,,,X,X,,,,
10201,CORMACK,U,0,,P,NEWFOUNDLAND AND LABRADOR,CORMACK,CORMACK,CANADA,A0K,A0K 2E0,-3.5,49.26222,-57.45583,120,,,,,,,,,,,,,,,,,,,,,,,,,,
10301,CREDIT UNION,C,0,,P,NEWFOUNDLAND AND LABRADOR,BROOK STREET,CORNER BROOK,CANADA,A2H,A2H 2T7,-3.5,48.949479,-57.945387,5,X,X,X,X,X,X,,,,,,,,,,,,X,,,,,,,,
... etc ...
```

### Ozone measurements (10MB, 74064 records for 2012)

- 24 Ozone readings per day per StationID
- 365 records per StationID
- "-999" means "missing data"

```
PC Stat  YYYYMMDD AVG MIN MAX H01 H02 H03 H04 H05 H06 H07 H08 H09 H10 H11 H12 H13 H14 H15 H16 H17 H18 H19 H20 H21 H22 H23 H24
00701010220120101  27  17  36  17  21  22  29  28  30  31  32  33  36  32  27  26  22  26  20  21  21  21  29  33  33  34  33
00701010220120102  30  25  37  33  33  32  31  27  29  29  29  27  25  29  27  28  26  27  29  29  32  32  33  34  35  37  36
00706440120120207-999  28  32  32  31  29  28  28  28  31  30  29-999-999-999-999-999-999-999-999-999-999-999-999-999-999-999
00706410120120925   9   3  19  11  11  10   7   7   6   3   4   8   8-999-999-999-999-999-999  19  15  11  10  11  10   9   8
00706410120120926  13   3  23   7   5   7-999-999  11   3   3  16-999-999  22  23  21  20  13  18  18  15  14  14  11  12   4
00706410120120927  16   2  27   9   8  10  10   9   7   6   2   5  17-999-999  22  23  25  27  27  24  22  22  19  21  21  21
00706410120120928  22   8  33  20  18  16  13  15-999  14  10   8  16  23  26  30  33  33  33  33  31  27  27  26  25  23  16
... etc ...
```

### The "Most Polluted Cities in Canada" report

- Parsing the Ozone file
- Join StationID to Station MetaData, Substitute StationID by Province, CityName
- Group By (Province, CityName)
- Sort Grouping Result by (YearAvgOzone DESC, YearMaxOzone DECS)

```
       Province            CityName          YearAvgOzone  YearMaxOzone
  1    ALBERTA             Hightower Ridge        46.6     65
  2    QUEBEC              Sutton                 35.69    62
  3    NOVA SCOTIA         Aylesford              33.47    55
  4    ONTARIO             Grand Bend             33.23    74
  5    ONTARIO             Port Stanley           33.07    68
  6    ONTARIO             Simcoe                 32.84    74
  7    NOVA SCOTIA         Sable Island           32.8     56
  8    QUEBEC              Frelighsburg           32.77    63
  9    ONTARIO             Kingston               32.71    67
 10    QUEBEC              St-Hilaire-De-Dorset   32.62    63
... etc ...
164    BRITISH COLUMBIA    Squamish               17.98    37
165    BRITISH COLUMBIA    Metro Van - Delta      17.97    40
166    BRITISH COLUMBIA    Metro Van - Coquitlam  17.78    40
167    BRITISH COLUMBIA    Metro Van - North Van. 16.84    39
168    NOVA SCOTIA         Halifax                14.99    35
169    BRITISH COLUMBIA    Metro Van - Port Moody 14.96    38
170    BRITISH COLUMBIA    Metro Van - Vancouver  14       40
```

2. THE DESIGN SOLUTIONS
=======================

The idea of this project is to show how to use BigData Development design to 
implement the same solution as "traditional" relational technique.
A conscencious effort has been made to use the "best practices" corresponding to each tool.
   
### SQL Server
This is used as "reference" design, to which the BigData solutions must be compete
in terms of design time and execution time. This solution uses the best of breed tool & design SQLServer 2012R2 / 2014:

- ETL: SQLServer Integration Service package 
- Query: T-SQL scripts (hope that you will appreciate the geekness of these scripts)

The SQLServer solution is not runable or editable under Hadoop Dev environment.
The SQLServer solution is supplied here in compressed format as courtesy in case you are interested.  
You will need to uncompress and use a dev environment of SQLServer 2012R2 or SQLServer 2014
to open review and run the solution. 


### Hive
The Hive implementation solves the solution the "Hive" way.
It is not an attempt to make it look like SQL and solve the SQL way

### Pig
The Pig implentation uses Pig's own feature to solve the solution
For the scenario at hand, the Pig implementation is actually the most elegant

### Java MapReduce
Java MapReduce uses the Hadoop 2.40 API (as of July 2014)
Although this is a pretty well designed Java MapReduce implementation. The solution is NOT optimal.
Areas of possible improvement include:
   
- Better way to chaine 2 jobs
- Better way to join datasets
- Sort the outputs of the report to make it look like other solutions

As of July 2015, this JavaMR is no longer maintained. Mainly because it is very heavy on maintenance
and yet having quite low performance compared to a comparable solution using Spark and Scala


### R Programming
Although not really a "BigData" design. R is a wonderful tool to solve problems similar to that of this project.
As a matter of fact, the R solution shines in every area:

- Fastest in design time (2 to 3 hours)
- Fastest in performance (less than 2 secs)
- Can produce a graphical report with a few lines of code, this is something that none of the other solutions can do
  See /Screenshots/R_*.png

I hope you will appreciate the quality of the R Code. It uses state-of-the-art R Programming techniques
(as of July 2015): readr, dplyr, datatable. I have consulted the experts on StackOverflow for the geeky programming details.


### Spark
This is the closest solution to the "Java MapReduce" design above. As Spark also uses the MapReduce semantics
and has the same distributed computing scalability than Java MapReduce.

However, the Spark API is more generic and abstract out many low level details such as class serialization, 
client code distribution, separate driver/map/reducer classes. In addition the Scala language cuts down significantly
the amount of Java code. As a matter of fact, after designing the Spark solution, I have decided to discontinue
the support of the Java MapReduce solution.


3. DEV ENVIRONMENT
==================
- OS: Xubuntu 14.04 64 bits
- Java: OpenJDK 1.7.0_79 64 bits
- IntelliJ IDEA Community Edition v14.1.5 : http://www.jetbrains.com/idea/
- Git 2.4.3
- Hadoop 2.40: http://mirror.csclub.uwaterloo.ca/apache/hadoop/common/hadoop-2.4.0/hadoop-2.4.0.tar.gz
- Maven 3.2.1: http://maven.apache.org/download.cgi
- R v3.20, RStudio 0.99.473 (latest as of Sept 2015)
- Spark 1.5.0 (released on 2015-09-09), prebuilt for Hadoop 2.40, Scala 2.10.4

- Hadoop client node:
  this is where the Java MapReduce code, Pig and Hive scripts will be deployed and executed
  The Dev machine doesn't have Hadoop and cannot execute any of these codes.

  We'll use a "Pseudo Distributed Cluster" which is Hortonworks HDP Sandbox 2.2.4
  <http://hortonworks.com/products/hortonworks-sandbox/>

  NOTE: you must have the HDP Sandbox running using a static IP
  The code in this project assumes that the HDP Sandbox name and IP is:
  192.168.56.102   HDPSBHV # HDP Sandbox

  It is possible to use any name and IP. You just need to change the deployement statements accordingly
  Each code and script in this project is supplied with deployment instructions in the comment section

- Hadoop component versions (within the HDP Sandbox 2.2.4)
  . Hadoop 2.40
  . Pig 0.14
  . Hive 0.14


4. TEST RESULTS
===============
[Benchmark Results](../README.md)


5. PROJECT DIRECTORIES
======================

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

- src/main/R : R scripts (Data clean up, Analysis, Plotting Graphics)

- src/main/scala : Standalone Scala application using Spark API v1.40 

- src/test/java : JUnit Tests for the java solution


6. COPYRIGHTS
==============
The code is free and can be used for private or commercial purposes.
You don't need to ask for permission. However I would appreciate
if you mention the author name (Tri Nguyen) and the URL where you get the code.

In the spirit of knowledge sharing. If you have made any significant improvement to the code,
I would appreciate a pull request so that we can contribute to the benefits of all.

Thank you and enjoy Hadoop Programming.

Tri Nguyen

===== (end) =====
