## Description
2015-07-12 - Tri Nguyen, initial release

This is the Solution in Spark to be compared in design time & execution duration with
other Hadoop solutions in <https://github.com/NeuroNex/UG/tree/master/HadoopLab>

Analyse Ozone measurements of National Air Pollution Surveillance Program (NAPS)<br/>
Source Data: <http://maps-cartes.ec.gc.ca/rnspa-naps/data.aspx?lang=en>

- Across Canada, for the entire year 2012 
  (why 2012 in 2015? to make results comparable to Hadoop solutions which were designed earlier)
- Each NAPS Station has 1 Ozone record per day, each day has 24 reading of Ozone
- Compute the Average Ozone daily value (avg of 24 readings)
- Per City, for the entire year (i.e. the entire source data): Calculate the aggregates Max(DailyAvgOzone), Avg(DailyAvgOzone)
- Make a Report "Most poluted cities", ranking by MaxAvgOzone descending

## Data Setup
[Instructions common to all solutions](../Data_SETUP.md)

Create a HDFS dir for temp storage
```
# hdfs dfs -mkdir -p /user/tri/AirAnalysis/Spark/
```


## Build & Run (LOCAL, DEV MACHINE)
### Dev environment (2015-07-12)
- Spark 1.4.0 (released on June 11, 2015), prebuilt for Hadoop 2.40, Scala 2.10.4
- Java: OpenJDK 1.7.0_79 64 bits
- IntelliJ IDEA Community Edition v14.1.4

### Build, Run locally
Scala_Project_Directory contains:
- the Scala bild file **build.sbt**
- src/main/scala directory
```
$ cd ~/Documents/IntelliJProjects/UG/HadoopLab/AirQualityAnalysis
$ sbt clean package
```

[Run application locally on 4 cores](https://spark.apache.org/docs/latest/submitting-applications.html)
```
$ spark-submit --class hadooplab.airquality.OzoneAnalysis --master local[4] \
  ./target/scala-2.10/sparkozoneanalysis_2.10-1.jar \
  ./src/main/resources/Stations_v28012014.csv ./src/main/resources/2012O3.hly \
  ./Results/TempSpark ./Results/OzoneByCities_Spark_Local.txt
```

## DEPLOY ON SPARK CLUSTER
"$" means command line to be executed from the Dev machine
"#" means command line to be executed on the Hadoop node 

Deploy Scala App to Spark Node:
```
$ cd ~/Documents/IntelliJProjects/UG/HadoopLab/AirQualityAnalysis
$ scp ./target/scala-2.10/sparkozoneanalysis_2.10-1.jar root@hdpsbhv:/root/HadoopExo/
```

Confirm hostname by
```
# echo -e "IP:`hostname -i`\t\tShortHostname:`hostname -s`\tFQDN:`hostname -f`"
```

Delete dir/file saved by previous execution<br/>
**NO NEED** (it is done automatically in the code)
```
# hdfs dfs -rm -R -skipTrash /user/tri/AirAnalysis/Spark/Temp
# hdfs dfs -rm -skipTrash /user/tri/AirAnalysis/Spark/OzoneByCities_Spark_HDFS.txt
```
Start Scala App:
```
# spark-submit --class hadooplab.airquality.OzoneAnalysis /root/HadoopExo/sparkozoneanalysis_2.10-1.jar \
  hdfs://sandbox.hortonworks.com:8020/user/tri/AirAnalysis/NAPSStation/Stations_v28012014.csv \
  hdfs://sandbox.hortonworks.com:8020/user/tri/AirAnalysis/Ozone2012RawFL/2012O3.hly \
  hdfs://sandbox.hortonworks.com:8020/user/tri/AirAnalysis/Spark/Temp \
  hdfs://sandbox.hortonworks.com:8020/user/tri/AirAnalysis/Spark/OzoneByCities_Spark_HDFS.txt
```

Review Results:
```
# hdfs dfs -cat /user/tri/AirAnalysis/Spark/OzoneByCities_Spark_HDFS.txt
# rm -f /root/HadoopExo/OzoneByCities_Spark_HDFS.txt
# hdfs dfs -copyToLocal /user/tri/AirAnalysis/Spark/OzoneByCities_Spark_HDFS.txt /root/HadoopExo/
$ scp root@hdpsbhv:/root/HadoopExo/OzoneByCities_Spark_HDFS.txt ~/MyLocalDir/
```

--- (end) ---
