## Data Setup for OzoneAnalysis
The source data files are copied on HDFS and will be used by the following solutions:

- Hive
- Pig
- Java
- Spark (Scala)

"$" means command line to be executed from the Dev machine
"#" means command line to be executed on the Hadoop node 

	$ scp ~/Documents/IntelliJProjects/BigDataLAB/13_AirQualityAnalysis/src/main/resources/Stations_v28012014.csv root@hdpsbhv:/root/HadoopExo/
	# hdfs dfs -mkdir -p /user/tri/AirAnalysis/NAPSStation/
	# hdfs dfs -copyFromLocal -f /root/HadoopExo/Stations_v28012014.csv /user/tri/AirAnalysis/NAPSStation/

	$ scp ~/Documents/IntelliJProjects/BigDataLAB/13_AirQualityAnalysis/src/main/resources/2012O3.hly root@hdpsbhv:/root/HadoopExo/
	# hdfs dfs -mkdir -p /user/tri/AirAnalysis/Ozone2012RawFL/
	# hdfs dfs -copyFromLocal -f /root/HadoopExo/2012O3.hly /user/tri/AirAnalysis/Ozone2012RawFL/

--- (end) ---
