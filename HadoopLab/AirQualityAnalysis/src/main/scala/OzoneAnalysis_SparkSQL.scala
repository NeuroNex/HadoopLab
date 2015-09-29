/*
Solution using **SparkSQL**.
This script WILL NOT compile, must execute line by line in spark-shell

The business logic is intentionally simplified:
- re-use the precalculated Ozone Average (no row-wise average calculation)
- use DataFrame instead of MapReduce to produce the report of "Ozone Pollution by Cities"

2015-09-25 - Tri.Nguyen
*/

/*
------------------------------------------------------------------------------------------------
STEP 1: Station RDD: (StationID, StationName, Province, CityName)

stationRows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[51] at map at <console>:27
------------------------------------------------------------------------------------------------

Source: National Air Pollution Surveillance Program (NAPS) <http://maps-cartes.ec.gc.ca/rnspa-naps/data.aspx?lang=en>
Online:
<https://github.com/NeuroNex/UG/blob/master/HadoopLab/AirQualityAnalysis/src/main/resources/Stations_v28012014.csv>
<https://github.com/NeuroNex/UG/blob/master/HadoopLab/AirQualityAnalysis/src/main/resources/2012O3.hly>

NAPS Station file:
  0        1           2     3     4      5          6           7          8
NAPS_ID,STATION_NAME,Type,Status,TOXIC,Designated,PROVINCE,STREET_ADDRESS,CITY,COUNTRY,FSA,Postal_Code,TimeZone,Lat_Decimal,Long_Decimal,Elevation_m,SO2,CO,NO2,NO,NOX,O3,PM_10_continuous,PM_25_015_continuous,PM_25_017_dryer,PM_25_018_BAM_RH45,PM_25_019_gravimetric40/50,PM_25_020_gravimetric30/50,PM_25_021_gravimetric30,PM_25_022__FDMS,PM_1_GRIMM180,PM_25_026_GRIMM180,PM_10_GRIMM180,PM_25_030_BAM_RH3,PM_10_031_BAM_RH35,PM_25_032_SHARP,VOC,ALDEH_KETO,DICHOTX_METALS,PCB,PCDD,PBDE
10101,DUCKWORTH & ORDINANCE,C,0,,P,NEWFOUNDLAND AND LABRADOR,DUCKWORTH & ORDINANCE,ST. JOHN'S,CANADA,A1C,A1C 1E4,-3.5,47.56806,-52.70222,7,X,X,X,X,X,X,,,,,,,,,,,,,,,,,,,,
40101,WOODSTOCK ROAD,C,0,,P,NEW BRUNSWICK,WOODSTOCK ROAD,FREDERICTON,CANADA,E3B,E3B 2L7,-4,45.96556,-66.65361,10,,,,,,,,,,,,,,,,,,,,,,,,,,
50125,CENTRE R\C9CR\C9ATIF EDOUARD RIVEST,R,0,,P,QUEBEC,11111 NOTRE-DAME EST,MONTREAL,CANADA,H1B,H1B 2V7,-5,45.62667,-73.5,10,,,,,,,,,,,,,,,,,,,,,,,X,,,
*/


import org.apache.spark.sql.Row
val rawStation = sc.textFile("/home/tri/Documents/IntelliJProjects/UG/HadoopLab/AirQualityAnalysis/src/main/resources/Stations_v28012014.csv").filter(_.matches("^\\d+,.+$"))
val stationRows = rawStation.map(_.split(",")).map(a => Row(a(0).toInt, a(1).toLowerCase.capitalize, a(6), a(8).toLowerCase.capitalize))

/*
stationRows.takeSample(false, 12, 20150925).foreach(r => printf("%6s Station:%-30s Prov:%-20s City:%s\n", r(0), r(1), r(2), r(3)))
>	100111 Station:Rocky pt. park                 Prov:BRITISH COLUMBIA     City:Metro van - port moody
	 41001 Station:Campobello island              Prov:NEW BRUNSWICK        City:Campobello island
	 30119 Station:Harbourview school             Prov:NOVA SCOTIA          City:Halifax
	129303 Station:Waterlab                       Prov:NUNAVUT              City:Iqaluit
	100401 Station:Federal building               Prov:BRITISH COLUMBIA     City:Kamloops
	 80302 Station:110 ominica street             Prov:SASKATCHEWAN         City:Moosejaw
	101901 Station:Cranbrook pr3                  Prov:BRITISH COLUMBIA     City:Cranbrook
	 70105 Station:Martin & henderson hwy         Prov:MANITOBA             City:Winnipeg
	100106 Station:2294 west 10th avenue          Prov:BRITISH COLUMBIA     City:Gvrd - vancouver
	100302 Station:1106 cook st.                  Prov:BRITISH COLUMBIA     City:Victoria
	 60501 Station:Woodlands park                 Prov:ONTARIO              City:Hamilton
	100201 Station:Post office                    Prov:BRITISH COLUMBIA     City:Prince george
*/


/*
------------------------------------------------------------------------------------------------
STEP 2: Ozone RDD: (StationID, Date, PreCalcAVROzone)

ozoneRows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[19] at map at <console>:24
------------------------------------------------------------------------------------------------

>OZONE: Fixed Length Columns definition:
PC Stat  YYYYMMDD AVG MIN MAX H01 H02 H03 H04 H05 H06 H07 H08 H09 H10 H11 H12 H13 H14 H15 H16 H17 H18 H19 H20 H21 H22 H23 H24
00701010220120102  30  25  37  33  33  32  31  27  29  29  29  27  25  29  27  28  26  27  29  29  32  32  33  34  35  37  36
00706440120120207-999  28  32  32  31  29  28  28  28  31  30  29-999-999-999-999-999-999-999-999-999-999-999-999-999-999-999
00706410120120926  13   3  23   7   5   7-999-999  11   3   3  16-999-999  22  23  21  20  13  18  18  15  14  14  11  12   4
00706410120120927  16   2  27   9   8  10  10   9   7   6   2   5  17-999-999  22  23  25  27  27  24  22  22  19  21  21  21
*/
//val ozoneRdd = rawOzone.map { line => (line.substring(3,9).toInt, (line.substring(9,17), line.substring(17,21).trim.toInt ))}
//ozoneRdd.takeSample(false, 10, seed=20150925).
//  foreach{ case (stationID, (date, avgOzone)) => println(f"$stationID%06d: $date%s - AVGOzone:$avgOzone%3d")}

val rawOzone = sc.textFile("/home/tri/Documents/IntelliJProjects/UG/HadoopLab/AirQualityAnalysis/src/main/resources/2012O3.hly").filter(_.matches("^\\d{17}.+$"))
val ozoneRows = rawOzone.filter(_.substring(17,21) != "-999").
  map { line => Row(line.substring(3,9).toInt, line.substring(9,17), line.substring(17,21).trim.toFloat)}


//ozoneRows.takeSample(false, 10, seed=20150925).foreach(println)
ozoneRows.takeSample(false, 10, seed=20150925).foreach(r => printf("%6s, %8s - AVGOzone:%5s\n", r(0), r(1), r(2)))
/*
	106800, 20120305 - AVGOzone: 39.0
	 60809, 20120926 - AVGOzone: 14.0
	 40801, 20120304 - AVGOzone: 33.0
	 10102, 20120223 - AVGOzone: 33.0
	 60433, 20120309 - AVGOzone: 30.0
	 90222, 20120401 - AVGOzone: 42.0
	 90601, 20121209 - AVGOzone:  4.0
	100111, 20120527 - AVGOzone: 31.0
	 92601, 20120818 - AVGOzone: 35.0
	 60410, 20121122 - AVGOzone: 15.0
*/


/*------------------------------------------------------------------------------------------------
   STEP 3: Define Dataframe with Static Schema
------------------------------------------------------------------------------------------------*/

import org.apache.spark.sql.types._

val schemaOzone = StructType(Array(
  StructField(name="StationID", dataType=IntegerType, nullable=false),
  StructField("Date", StringType, false),
  StructField("DayAvgOzone", FloatType, false)
))

val dfOzone = sqlContext.createDataFrame(ozoneRows, schemaOzone)

dfOzone.printSchema
/*
root
	 |-- StationID: integer (nullable = false)
	 |-- Date: string (nullable = false)
	 |-- DayAvgOzone: float (nullable = false)
*/

dfOzone.sample(withReplacement=false, fraction=0.005, seed=20150925).show(7)
/*
	+---------+--------+------------+
	|StationID|    Date|AverageOzone|
	+---------+--------+------------+
	|    10401|20120829|        30.0|
	|    10501|20120207|        29.0|
	|    10501|20120310|        37.0|
	|    10501|20120511|        34.0|
	|    10501|20120820|        25.0|
	|    10501|20121101|        30.0|
	|    10601|20120524|        32.0|
	+---------+--------+------------+
*/

val schemaStation = StructType(Array(
  StructField(name="StationID", dataType=IntegerType, nullable=false),
  StructField("StationName", StringType, false),
  StructField("Province", StringType, false),
  StructField("CityName", StringType, false)
))

val dfStation = sqlContext.createDataFrame(stationRows, schemaStation)
dfStation.printSchema
/*
	root
	 |-- StationID: integer (nullable = false)
	 |-- StationName: string (nullable = false)
	 |-- Province: string (nullable = false)
	 |-- CityName: string (nullable = false)
*/

dfStation.sample(withReplacement=false, fraction=0.01, seed=20150925).show(7)
/*
	+---------+--------------------+----------------+-----------------+
	|StationID|         StationName|        Province|         CityName|
	+---------+--------------------+----------------+-----------------+
	|    60802|     185 gore street|         ONTARIO|      Thunder bay|
	|    90120|      Edmonton south|         ALBERTA|         Edmonton|
	|    90222|   Calgary northwest|         ALBERTA|          Calgary|
	|   100140|           Tsawwasen|BRITISH COLUMBIA|Metro van - delta|
	|   100204|     Van bien school|BRITISH COLUMBIA|    Prince george|
	|   100315|Victoria christop...|BRITISH COLUMBIA|         Victoria|
	|   103203|          Golden cpr|BRITISH COLUMBIA|           Golden|
	+---------+--------------------+----------------+-----------------+
*/

/*------------------------------------------------------------------------------------------------
   STEP 4: Analysis using SQL
------------------------------------------------------------------------------------------------*/

dfOzone.registerTempTable("Ozone")
dfStation.registerTempTable("Station")

val dfPollutionByCity = sqlContext.sql("SELECT RANK() OVER (ORDER BY AVG(O.DayAvgOzone) DESC) AS Rank, " +
  "S.Province, S.CityName, ROUND(AVG(O.DayAvgOzone),2) as YearAvgOzone, INT(MAX(O.DayAvgOzone)) as YearMaxOzone " +
  "FROM Ozone O INNER JOIN Station S ON S.StationID = O.StationID " +
  "GROUP BY S.Province, S.CityName " +
  "ORDER BY YearAvgOzone DESC, YearMaxOzone DESC")
// dfPollutionByCity: org.apache.spark.sql.DataFrame = [Rank: int, Province: string, CityName: string, YearAvgOzone: double, YearMaxOzone: int]


dfPollutionByCity.show(15, truncate=false)
/*
	+----+----------------+--------------------+------------+------------+
	|Rank|Province        |CityName            |YearAvgOzone|YearMaxOzone|
	+----+----------------+--------------------+------------+------------+
	|1   |ALBERTA         |Hightower ridge     |46.69       |65          |
	|2   |QUEBEC          |Sutton              |35.67       |62          |
	|3   |NOVA SCOTIA     |Aylesford           |33.48       |55          |
	|4   |ONTARIO         |Grand bend          |33.25       |74          |
	|5   |ONTARIO         |Port stanley        |33.07       |68          |
	|6   |NOVA SCOTIA     |Sable island        |32.92       |56          |
	|7   |ONTARIO         |Simcoe              |32.86       |74          |
	|8   |QUEBEC          |Frelighsburg        |32.81       |63          |
	|9   |ONTARIO         |Kingston            |32.8        |67          |
	|10  |QUEBEC          |St-hilaire-de-dorset|32.62       |63          |
	|11  |ALBERTA         |Caroline            |32.51       |56          |
	|12  |QUEBEC          |Tingwick            |32.12       |58          |
	|13  |ONTARIO         |Tiverton            |32.01       |66          |
	|14  |BRITISH COLUMBIA|Ucluelet            |31.91       |51          |
	|15  |QUEBEC          |Stukely-sud         |31.87       |59          |
	+----+----------------+--------------------+------------+------------+
*/


/*------------------------------------------------------------------------------------------------
   STEP 5: Save DataFrame as CSV
------------------------------------------------------------------------------------------------*/


// Delete output dir if already exists
val tmpOutputDir = "/home/tri/AAA/OzoneAnalysisSpark"
org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(tmpOutputDir))
dfPollutionByCity.map(a => a.mkString("\t")).saveAsTextFile(tmpOutputDir)

// a file named "part-00000" is created in tmpOutputDir, rename + move it to final location 
val oldFile = new java.io.File(tmpOutputDir + "/part-00000")
val newFile = new java.io.File("/home/tri/AAA/OzoneByCities_SparkSQL.txt")
oldFile.renameTo(newFile)

//───(end)───
