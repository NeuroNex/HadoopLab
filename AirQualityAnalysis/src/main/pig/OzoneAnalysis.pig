/*==========================================================
Toronto User SQL Group - http://toronto.sqlpass.org/
Session April 2014 - "Discovery of Hadoop Under a Relational Lens Scope"

Analyse Ozone measurements of 2012 from the Canadian National Air Pollution Surveillance Program (NAPS)
Then produce a report "Pollution by Cities"

Data Preparation:
   $ scp /home/tri/Documents/HadoopLab/AirQualityAnalysis/src/main/resources/2012O3.hly root@hdpsbhv:/root/HadoopExo/
   $ scp /home/tri/Documents/HadoopLab/AirQualityAnalysis/src/main/resources/Stations_v28012014.csv root@hdpsbhv:/root/HadoopExo/
   # hdfs dfs -mkdir -p /user/Tri/AirAnalysis/Ozone2012RawFL/
   # hdfs dfs -mkdir -p /user/Tri/AirAnalysis/NAPSStation/
   # hdfs dfs -copyFromLocal -f /root/HadoopExo/2012O3.hly /user/Tri/AirAnalysis/Ozone2012RawFL/
   # hdfs dfs -copyFromLocal -f /root/HadoopExo/Stations_v28012014.csv /user/Tri/AirAnalysis/NAPSStation/

Deploy Code
   $ scp /home/tri/Documents/HadoopLab/AirQualityAnalysis/src/main/pig/OzoneAnalysis.pig root@hdpsbhv:/root/HadoopExo/Pig/

EXEC (HDFS mode)
   # pig OzoneAnalysis.pig

2014-04-17 - Tri Nguyen
==========================================================*/

/*-----------------------------------------------
  STEP1:
  - The Raw Data 1 entire line (fixed length)
  - Remove invalid Records
-----------------------------------------------*/

RawFL = LOAD '/user/Tri/AirAnalysis/Ozone2012RawFL/2012O3.hly' AS (line:chararray);

--Remove invalid records (Good ones have StationID > 0)
RawFL2 = FILTER RawFL BY (int) SUBSTRING(line, 3, 9) > 0;


/*-----------------------------------------------
  STEP2:
  - Split fixed length line into columns
  - Strong type

PC Stat  YYYYMMDD AVG MIN MAX H01 H02 H03 H04 H05 H06 H07 H08 H09 H10 H11 H12 H13 H14 H15 H16 H17 H18 H19 H20 H21 H22 H23 H24
00701010220120105  23   5  29  26  25  25  28  28  27  29  28  26  26  26  27-999-999-999-999  23  26  24  22  16  10   8   5
00701010220120106  25  17  31  19  31  29  29  29  30  31  30  26  20  17-999  24  24  24  24  24  21  23  24  26  23  23  27
-----------------------------------------------*/

--MiniRawFL = LIMIT RawFL2 50;

--ATTN: substring index is 0-based
OZ = FOREACH RawFL2 GENERATE
--(chararray) SUBSTRING(line, 0, 3) AS PollutantCode, --not used, we always do Ozone in this scenario
(int) SUBSTRING(line, 3, 9) AS StationID,
--(DateTime) ToDate(SUBSTRING(line, 9, 17), 'yyyyMMdd') AS MeasureDate, --OverKill we don't do anything with strong date type
(chararray) SUBSTRING(line, 9, 17) AS MeasureDate,
(int) REPLACE(SUBSTRING(line, 17, 21),'-999', '') AS DayAverage,
(int) REPLACE(SUBSTRING(line, 21, 25),'-999', '') AS DayMin,
(int) REPLACE(SUBSTRING(line, 25, 29),'-999', '') AS DayMax,
TOBAG(
	(int) REPLACE(SUBSTRING(line, 29, 33),'-999', ''),
	(int) REPLACE(SUBSTRING(line, 33, 37),'-999', ''),
	(int) REPLACE(SUBSTRING(line, 37, 41),'-999', ''),
	(int) REPLACE(SUBSTRING(line, 41, 45),'-999', ''),
	(int) REPLACE(SUBSTRING(line, 45, 49),'-999', ''),
	(int) REPLACE(SUBSTRING(line, 49, 53),'-999', ''),
	(int) REPLACE(SUBSTRING(line, 53, 57),'-999', ''),
	(int) REPLACE(SUBSTRING(line, 57, 61),'-999', ''),
	(int) REPLACE(SUBSTRING(line, 61, 65),'-999', ''),
	(int) REPLACE(SUBSTRING(line, 65, 69),'-999', ''),
	(int) REPLACE(SUBSTRING(line, 69, 73),'-999', ''),
	(int) REPLACE(SUBSTRING(line, 73, 77),'-999', ''),
	(int) REPLACE(SUBSTRING(line, 77, 81),'-999', ''),
	(int) REPLACE(SUBSTRING(line, 81, 85),'-999', ''),
	(int) REPLACE(SUBSTRING(line, 85, 89),'-999', ''),
	(int) REPLACE(SUBSTRING(line, 89, 93),'-999', ''),
	(int) REPLACE(SUBSTRING(line, 93, 97),'-999', ''),
	(int) REPLACE(SUBSTRING(line, 97,101),'-999', ''),
	(int) REPLACE(SUBSTRING(line,101,105),'-999', ''),
	(int) REPLACE(SUBSTRING(line,105,109),'-999', ''),
	(int) REPLACE(SUBSTRING(line,109,113),'-999', ''),
	(int) REPLACE(SUBSTRING(line,113,117),'-999', ''),
	(int) REPLACE(SUBSTRING(line,117,121),'-999', ''),
	(int) REPLACE(SUBSTRING(line,121,125),'-999', '')
) AS HourReadings;


/*-----------------------------------------------
  STEP3: Calculate AVG, MIN, MAX of 24 hours readings (of Ozone measures)

NOTE: the Pig's UDF AVG, MIN, MAX expect an argument of numeric values in a single-column bag
The kind of things we get in a relation created by "GROUP BY". Here we don't need because
luckily the HourReadings column was already a bag type thanks to the ToBAG() in previous step
-----------------------------------------------*/

O3Calc = FOREACH OZ GENERATE StationID,
	DayAverage, (int) AVG(HourReadings) AS AVGCalc,
	DayMin, MIN(HourReadings) AS MINCalc,
	DayMax, MAX(HourReadings) AS MAXCalc; --HourReadings;

--DUMP O3Calc;

/*-----------------------------------------------
  STEP4: Load NAPS Station Metadata

This relation will give us the location of the measures via the StationID
We are only interested in StationID, ProvinceName, City
-----------------------------------------------*/

NAPSStationORIG = LOAD '/user/Tri/AirAnalysis/NAPSStation/Stations_v28012014.csv' USING PigStorage(',')
AS (StationID:int, StationName:chararray, StationType:chararray, Status:chararray, Toxic:chararray, Designated:chararray,
   ProvinceName:chararray, StreetAddr:chararray, City:chararray, Country:chararray, FSA:chararray, PostalCode:chararray,
   Timezone:float, Latitude:float, Longitude:float, ElevationMeter:int);

--DUMP NAPSStation;
--DESCRIBE NAPSStation;

StationCity = FOREACH NAPSStationORIG GENERATE StationID, ProvinceName, City;



-----------------------------------------------
-- 5.1 Join Ozone Measures with Station
-----------------------------------------------
--JO3Station: {O3Calc::StationID: int,O3Calc::DayAverage: int,O3Calc::AVGCalc: int,O3Calc::DayMin: int,O3Calc::MINCalc: int,O3Calc::DayMax: int,O3Calc::MAXCalc: int,StationCity::StationID: int,StationCity::ProvinceName: chararray,StationCity::City: chararray}
JO3Station = JOIN O3Calc BY StationID, StationCity BY StationID;

--JResults = FOREACH JO3Station GENERATE StationCity::ProvinceName, StationCity::City, O3Calc::AVGCalc;
--DUMP JResults;

-----------------------------------------------
-- 5.2 Group Results by Province, Cities
-----------------------------------------------
--GrpProvCity: {group: (StationCity::ProvinceName: chararray,StationCity::City: chararray),JO3Station: {(O3Calc::StationID: int,O3Calc::DayAverage: int,O3Calc::AVGCalc: int,O3Calc::DayMin: int,O3Calc::MINCalc: int,O3Calc::DayMax: int,O3Calc::MAXCalc: int,StationCity::StationID: int,StationCity::ProvinceName: chararray,StationCity::City: chararray)}}
GrpProvCity = GROUP JO3Station BY (StationCity::ProvinceName, StationCity::City);


-----------------------------------------------
-- 5.3 Extract final results: Province, Cities, AvgCalc
-- NOTE: use FLATTEN(group) to split the multikeys group in to individual fields
-----------------------------------------------
--Results: {group::StationCity::ProvinceName: chararray,group::StationCity::City: chararray,MaxAVGCalc: int}
Results = FOREACH GrpProvCity GENERATE FLATTEN(group), MAX(JO3Station.O3Calc::AVGCalc) AS MaxAVGCalc;

-----------------------------------------------
-- 5.4 Sort By MaxAVGCalc DESC
-----------------------------------------------
SortResults = ORDER Results BY MaxAVGCalc DESC, ProvinceName, City;

rmf /user/Tri/AirAnalysis/PigResults;
STORE SortResults INTO '/user/Tri/AirAnalysis/PigResults' USING PigStorage('\t') ;


/*
	# hdfs dfs -cat /user/Tri/AirAnalysis/PigResults/part-r-00000
	# rm -f ./OzoneByCities_Pig.txt
	# hdfs dfs -copyToLocal /user/Tri/AirAnalysis/PigResults/part-r-00000 ./OzoneByCities_Pig.txt

	--Use that result for Hive exercises
   $ scp root@hdpsbhv:/root/HadoopExo/PigExo/OzoneByCities_Pig.txt /home/tri/Documents/HadoopLab/AirQualityAnalysis/Results/
*/
