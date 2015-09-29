## Description
2014-04-19 - Tri Nguyen, inital release
2015-07-11 - Update with Beeline output, reorg doc

This is the Solution in HIVE to be compared in design time & execution duration with
other Hadoop solutions in <https://github.com/NeuroNex/UG/tree/master/HadoopLab>

Analyse Ozone measurements of National Air Pollution Surveillance Program (NAPS)
Source Data: <http://maps-cartes.ec.gc.ca/rnspa-naps/data.aspx?lang=en>

- Across Canada, for the entire year 2012 
  (why 2012 in 2015? to make results comparable to Hadoop solutions which were designed earlier)
- Each NAPS Station has 1 Ozone record per day, each day has 24 reading of Ozone
- Compute the Average Ozone daily value (avg of 24 readings)
- Per City, for the entire year (i.e. the entire source data): Calculate the aggregates Max(DailyAvgOzone), Avg(DailyAvgOzone)
- Make a Report "Most poluted cities", ranking by MaxAvgOzone descending

## Data Setup
Instructions common to all solutions: <file:///../Data_SETUP.md>

## Install custom Java UDF for HIVE
In the exercise, we'll need to compute the average of 24 columns. This operation is implemented via
a custom UDF designed in Java:

	Create JAR file: Ctrl-Shift-F9 (Menu Build / Make Module 13_AirQualityAnalysis)

	$ cd ~/Documents/IntelliJProjects/BigDataLAB/13_AirQualityAnalysis/target/classes/
	$ jar cfv MyHiveUDF.jar ./hadooplab/HiveUtil/*.class
	$ scp MyHiveUDF.jar root@hdpsbhv:/root/HadoopExo/Hive/
	# hdfs dfs -copyFromLocal -f /root/HadoopExo/Hive/MyHiveUDF.jar /user/tri/AirAnalysis/


## Using Beeline to execute Hive QL
Migrating from Hive CLI to Beeline: A Primer <http://blog.cloudera.com/blog/2014/02/migrating-from-hive-cli-to-beeline-a-primer/>
Beeline – New Command Line Shell <https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients>
SQLLine Manual <http://sqlline.sourceforge.net/#manual>

$ % bin/beeline

beeline> !connect jdbc:hive2://localhost:10000 root hadoop

... Run Hive QL statements in the * .hiveql files ...

**NOTE**: Contrary to Hive CLI, Beeline will not run HDFS command like
hive> dfs -ls /apps/hive/warehouse/ozonebycitiesresults/;
These statements should be run in the linux command line on a Hadoop node.

beeline> !quit


## Benchmarking:

| Step               | Duration (secs) | Description |
|--------------------|-----------------|-----------------------------------|
| Intermediate time1 |      25"        | Create tempOzone2012 table        | 
| Intermediate time2 |      13"        | Create Ozone2012 table            | 
| Intermediate time3 |      22"        | Create OzoneByCitiesResults table | 
| TOTAL              |      60"        |                                   | 


--- (end) ---
