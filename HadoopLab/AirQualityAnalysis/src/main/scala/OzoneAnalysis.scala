/*
This is the Solution in Spark + Scala to be compared in design time & execution duration with
- Earlier Hadoop solutions in <https://github.com/NeuroNex/UG/tree/master/HadoopLab>
- R Solutions (also designed in July 2015): <file:///~/Documents/RProjects/DaxylicDSS/zMyPRJ/OzoneAnalysis/>

Analyse Ozone measurements of National Air Pollution Surveillance Program (NAPS)
Source Data: <http://maps-cartes.ec.gc.ca/rnspa-naps/data.aspx?lang=en>

- Across Canada, for the entire year 2012
  (why 2012 in 2015? to make results comparable to Hadoop solutions which were designed earlier)
- Each NAPS Station has 1 Ozone record per day, each day has 24 reading of Ozone
- Compute the Average Ozone daily value (avg of 24 readings)
- Per City, for the entire year (i.e. the entire source data): Calculate the aggregates Max(DailyAvgOzone), Avg(DailyAvgOzone)
- Make a Report "Most poluted cities", ranking by MaxAvgOzone descending

Date Setup, Build, Test, Deploy: /src/main/scala/README.md

2015-07-12 - Tri Nguyen
*/

package hadooplab.airquality

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.fs._ // used by combineNFilesTo1
// Spark 1.21 (HDP 2.24) avoid compile error "value join is not a member of org.apache.spark.rdd.RDD"
import org.apache.spark.SparkContext._


object OzoneAnalysis {
  def main(args: Array[String]) {
    val startTimeMillis = System.currentTimeMillis()

    val napsStationFile = args(0)
    val naspOzoneFile = args(1)
    val tempOutputDir = args(2) // temporary output dir (to store intermediate results of RDD.saveAsTextFile())
    val outputCSVfilename = args(3) // the CSV containing the final RDD containing the results

    val conf = new SparkConf().setAppName("OzoneAnalysis")
    //val conf = new SparkConf().setMaster("local").setAppName("OzoneAnalysis")
    val sc = new SparkContext(conf)

    println("-----------------------------------------------------------------")
    println("Ozone Analysis, Spark version (Scala standalone app)\n")

    //------------------------------------------------------------------------
    println("STEP 1: Read NAPS Station Data")
    // Source: National Air Pollution Surveillance Program (NAPS) <http://maps-cartes.ec.gc.ca/rnspa-naps/data.aspx?lang=en>
    // Online: <https://github.com/NeuroNex/UG/blob/master/HadoopLab/AirQualityAnalysis/src/main/resources/Stations_v28012014.csv>
    //------------------------------------------------------------------------
/*
Format of a record in the NAPS Station file:
   0        1           2     3     4      5          6           7         8
NAPS_ID,STATION_NAME,Type,Status,TOXIC,Designated,PROVINCE,STREET_ADDRESS,CITY,COUNTRY,FSA,Postal_Code,TimeZone,Lat_Decimal,Long_Decimal,Elevation_m,SO2,CO,NO2,NO,NOX,O3,PM_10_continuous,PM_25_015_continuous,PM_25_017_dryer,PM_25_018_BAM_RH45,PM_25_019_gravimetric40/50,PM_25_020_gravimetric30/50,PM_25_021_gravimetric30,PM_25_022__FDMS,PM_1_GRIMM180,PM_25_026_GRIMM180,PM_10_GRIMM180,PM_25_030_BAM_RH3,PM_10_031_BAM_RH35,PM_25_032_SHARP,VOC,ALDEH_KETO,DICHOTX_METALS,PCB,PCDD,PBDE
10101,DUCKWORTH & ORDINANCE,C,0,,P,NEWFOUNDLAND AND LABRADOR,DUCKWORTH & ORDINANCE,ST. JOHN'S,CANADA,A1C,A1C 1E4,-3.5,47.56806,-52.70222,7,X,X,X,X,X,X,,,,,,,,,,,,,,,,,,,,
40101,WOODSTOCK ROAD,C,0,,P,NEW BRUNSWICK,WOODSTOCK ROAD,FREDERICTON,CANADA,E3B,E3B 2L7,-4,45.96556,-66.65361,10,,,,,,,,,,,,,,,,,,,,,,,,,,
*/
    //rawStation: org.apache.spark.rdd.RDD[String]
    val rawStation = sc.textFile(napsStationFile).filter(_.matches("^\\d+,.+$"))

    // Project the columns we need into a PairRDD: (StationID, tuple(StationName, Province, CityName))
    // stationRdd: org.apache.spark.rdd.RDD[(Int, (String, String, String))]
    val stationRdd = rawStation.map { x => val a = x.split(",") ; (a(0).toInt, (a(1).toLowerCase.capitalize, a(6), a(8).toLowerCase.capitalize))}

/*
//=====REVIEW (Spark Shell)=====
scala> stationRdd.takeSample(false, 10, seed=20150708).foreach(println)
*/


    //------------------------------------------------------------------------
    println("STEP 2: Read NAPS Ozone Measures")
    // Source: National Air Pollution Surveillance Program (NAPS) <http://maps-cartes.ec.gc.ca/rnspa-naps/data.aspx?lang=en>
    // Online: <https://github.com/NeuroNex/UG/blob/master/HadoopLab/AirQualityAnalysis/src/main/resources/2012O3.hly>
    //------------------------------------------------------------------------
/*
Format of a record in the NAPS Ozone file:
PC Stat  YYYYMMDD AVG MIN MAX H01 H02 H03 H04 H05 H06 H07 H08 H09 H10 H11 H12 H13 H14 H15 H16 H17 H18 H19 H20 H21 H22 H23 H24
00701010220120102  30  25  37  33  33  32  31  27  29  29  29  27  25  29  27  28  26  27  29  29  32  32  33  34  35  37  36
00706440120120207-999  28  32  32  31  29  28  28  28  31  30  29-999-999-999-999-999-999-999-999-999-999-999-999-999-999-999
00706410120120926  13   3  23   7   5   7-999-999  11   3   3  16-999-999  22  23  21  20  13  18  18  15  14  14  11  12   4
00706410120120927  16   2  27   9   8  10  10   9   7   6   2   5  17-999-999  22  23  25  27  27  24  22  22  19  21  21  21
*/
    val rawOzone = sc.textFile(naspOzoneFile).filter(_.matches("^\\d{17}.+$"))

    // split the fixed length record into string elements
    // ozoneSTR: org.apache.spark.rdd.RDD[(Int, (String, Array[String], Array[String]))] = MapPartitionsRDD[7] at map
    val ozoneSTR = rawOzone.map { line =>
      ( line.substring(3,9).toInt,
        (
          line.substring(9,17),
          Array(line.substring(17,21), line.substring(21,25), line.substring(25,29)),
          Array(line.substring(29,33), line.substring(33,37), line.substring(37,41), line.substring(41,45), line.substring(45,49),
            line.substring( 49, 53), line.substring( 53, 57), line.substring( 57, 61), line.substring( 61, 65), line.substring( 65, 69),
            line.substring( 69, 73), line.substring( 73, 77), line.substring( 77, 81), line.substring( 81, 85), line.substring( 85, 89),
            line.substring( 89, 93), line.substring( 93, 97), line.substring( 97,101), line.substring(101,105), line.substring(105,109),
            line.substring(109,113), line.substring(113,117), line.substring(117,121), line.substring(121,125))
        ))
    }

/*
//=====REVIEW (Spark Shell)=====
scala> ozoneSTR.foreach{ case (stationID, (date, arrPrecalc, arrOzone)) => printf("%06d: %s |%s| %s\n", stationID, date, arrPrecalc.mkString(" "), arrOzone.mkString(",")) }
*/


    // Convert array of 24 Ozone readings from string to proprer type.
    // Each resulting record is a tuple:
    // - (Int, (String, Array[Option[Int]], List[Int]))
    // - (StationID, (date, Array(PrecalcAvg, PrecalcMin, PrecalcMax), List(N Hours Ozone Reading))
    // PrecalcXXX means the value precalculated in the Ozone file
    // List(Daily Ozone Reading) has variable size, it only contains valid ozone reading (when string value was != "-999")

    // ozoneNUM: org.apache.spark.rdd.RDD[(Int, (String, Array[Option[Int]], List[Int]))] = MapPartitionsRDD[175] at map
    val ozoneNUM = ozoneSTR.map { case (stationID, (date, arrPrecalc, arrOzone)) =>
      val pc:Array[Option[Int]] = new Array[Option[Int]](arrPrecalc.length)
      var ozReadList:List[Int] = List[Int]()
      // 0 until arrPrecalc.length = arrPrecalc.indices
      for(i <- arrPrecalc.indices){ pc(i) = if (arrPrecalc(i) == "-999") None: Option[Int] else Some(arrPrecalc(i).trim.toInt) }
      arrOzone.foreach{ x => if (x != "-999") ozReadList ::= x.trim.toInt }
      (stationID, (date, pc, ozReadList))
    }

/*
//=====REVIEW (Spark Shell)=====
scala> ozoneNUM.takeSample(false, 10, seed=20150709).foreach{ case (stationID, (date, arrPrecalc, dailyOZreadings)) =>
printf("%06d: %s |%s| Valid Ozone Readings: %02d\n", stationID, date, arrPrecalc.mkString(" "), dailyOZreadings.size) }
*/


  //------------------------------------------------------------------------
    println("STEP 3: Calc Daily Min, Max, Avg on H01..H24 columns")
    //  **NOTE** this is a row-wise calculation on a fixed 24 values in dailyOZreadings
    //  This is NOT an aggregate calculation (in the sense of GroupBy)
    //------------------------------------------------------------------------

    // ozoneDailyCalc: org.apache.spark.rdd.RDD[(Int, (String, Array[Option[Any]]))] = MapPartitionsRDD[106] at map
    val ozoneDailyCalc = ozoneNUM.map{ case (stationID, (date, arrPrecalc, dailyOZreadings)) =>
      var smry:Array[Option[Any]] = new Array[Option[Any]](3)
      if (dailyOZreadings.isEmpty) {smry = Array(None, None, None) }
      else {
        smry = Array(Some(dailyOZreadings.min), Some(dailyOZreadings.max), Some(dailyOZreadings.sum.toDouble/dailyOZreadings.size))
      }
      (stationID, (date, smry))
    }

/*
//=====REVIEW (Spark Shell)=====
scala> ozoneDailyCalc.takeSample(false, 10, seed=20150709).foreach{ case (stationID, (date, mmaCalc)) =>
printf("%06d: %s Calc Min Max AVG=%5s%5s%8.2f\n", stationID, date, mmaCalc(0).getOrElse("n/a"), mmaCalc(1).getOrElse("n/a"), mmaCalc(2).getOrElse(-1.11))}
*/



  println("STEP 4: Ozone by Cities")
    //------------------------------------------------------------------------
    // STEP 4: Ozone by Cities
    // NOTE: there are legitimate repeated records per city because
    // - there are 365 record per StationID
    // - there might be more than one StationID per city
    //------------------------------------------------------------------------

    //---Join PairRDDs: Ozone Readings with Stations (join key = stationID)
    // ozoneCityJoined: org.apache.spark.rdd.RDD[(Int, ((String, Array[Option[Any]]), (String, String, String)))] = MapPartitionsRDD[89] at join
    val ozoneCityJoined = ozoneDailyCalc.join(stationRdd)

    // ozoneCity: org.apache.spark.rdd.RDD[((String, String), Any)] = MapPartitionsRDD[91] at map
    val ozoneCity = ozoneCityJoined.filter {
      // Discard records where DailyAvgCalc Ozone was not available (Option[Any] = None)
      case (stationID, (ozCalcTup, stationTup)) => ozCalcTup._2(2).isDefined //.isDefined means != None
    }.map {
      // Select only the useful info we need: Province, City, DailyAvgCalc
      case (stationID, (ozCalcTup, stationTup)) =>
      ((stationTup._2, stationTup._3), ozCalcTup._2(2).getOrElse(0.0))
    }

/*
//=====REVIEW (Spark Shell)=====
scala> ozoneCity.takeSample(false, 10, seed=20150709).foreach { case ((prov, city), dailyAvgOzone) =>
printf("%-35s: DailyAvgOzone =%7.2f\n", prov + ", " + city, dailyAvgOzone) }
*/

  //------------------------------------------------------------------------
    println("STEP 5: Yearly Aggregate By City")
    // Here "Year" means "the entire Ozone source file" b/c the Ozone data represents 1 full year of measurements)
    // - YearMaxOzone = the max value per Key of dailyAvgCalc
    // - YearAvgOzone = the average value per Key of dailyAvgCalc
    //------------------------------------------------------------------------

    // mapValues(dayAvg => (dayAvg, dayAvg, 1))
    // - Input : dayAvg means daily average ozone
    // - Output: tuple (max, sum, count) type (Double, Double, Int)
    //   NOTE: original dayAvg is Option[Double] must be converted to Double to allow math.max()

    // yearAggreg: org.apache.spark.rdd.RDD[((String, String), (Double, Double, Int))] = ShuffledRDD[114] at reduceByKey
    val yearAggreg = ozoneCity.mapValues {
      case dayAvg:Double => (dayAvg, dayAvg, 1)
      case _ => (0.0, 0.0, 1)
    }.reduceByKey((tup1, tup2) => (math.max(tup1._1, tup2._1), tup1._2 + tup2._2, tup1._3 + tup2._3))


    //------------------------------------------------------------------------
    println("STEP 6: Final report \"Most polluted cities\"")
    //------------------------------------------------------------------------

    // The report "most polluted cities" must be sorted by the VALUE YearMaxOzone
    // Need to re-arrange the PairRDD record format so that (YearMaxOzone, YearAvgOzne) is the key
    // NOTE: the Key is a tuple of 2 values to allow sorting on a composite key

    // aggKeybyValue: org.apache.spark.rdd.RDD[((Double, Double), (String, String))] = MapPartitionsRDD[115] at map
    val aggKeybyValue = yearAggreg.map{ case ((prov, city), (yearMax, yearCumul, count)) => ((yearCumul/count, yearMax), (prov, city))}

/*
//=====REVIEW (Spark Shell)=====
scala> aggKeybyValue.repartition(1).sortByKey(ascending=false).takeSample(false, 10, seed=20150709).
foreach { case ((yearAvg, yearMax), (prov, city)) => printf("%-35s %8.2f %8.2f\n", prov + ", " + city, yearAvg, yearMax)}
*/

    // Final report "most polluted cities"
    // - Added line number to allow easy comparison with results from R solutions (~/Documents/RProjects/DaxylicDSS/zMyPRJ/OzoneAnalysis/)
    // - Force partition = 1 to generate a unique sorted array
    // - Double values rounded to Integer so make results comparable with other solutions (in particular with R)
    var linenum = 0
    val pollutedCities = aggKeybyValue.repartition(1).sortByKey(ascending=false).map {
      case ((yearAvg, yearMax), (prov, city)) =>
      linenum += 1
        Array(linenum, prov, city, f"$yearAvg%.2f", f"$yearMax%.0f")
    }

/*
//=====REVIEW (Spark Shell)=====
scala> pollutedCities.takeSample(false, 10, seed=20150709).foreach{a => println(a.mkString("\t"))}
*/


    //------------------------------------------------------------------------
    println("STEP 7: Save to CSV")
    //------------------------------------------------------------------------

    deleteDirRecursive(tempOutputDir)
    deleteFile(outputCSVfilename)

    // Save RDD to 1 single CSV:
    // - Using parallel processing: there are N separate CSVs files (1 per partition)
    // - Use Hadoop HDFS copyMerge() function which squashes part files together into a single file.
    // ThankYou: http://www.markhneedham.com/blog/2014/11/30/spark-write-to-csv-file/
    pollutedCities.map(a => a.mkString("\t")).saveAsTextFile(tempOutputDir)
    printf("\tSaving RDD to [%s]\n", outputCSVfilename)
    combineNFilesTo1(tempOutputDir, outputCSVfilename)

    deleteDirRecursive(tempOutputDir)

    // elapsed time:
    // - local, default 2 tasks, (Desktop, CPU Q9550, 8MB RAM): 8.77 seconds
    // - HDFS , default 2 tasks, (VM HyperV 2012R2, 8GB RAM, AMD FX-8320): 8.77 seconds
    printf("\nFINISHED:%7.2f seconds\n", (System.currentTimeMillis() - startTimeMillis)/1000.0)
  }

  /**
   * Use HDFS I/O to combine separate files part-00000 .. part-N into one single file
   * NOTE1: NO-Need to add dependency in build.sbt (spark already comes with relevant Hadoop lib):
   *        libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.4.0"
   * NOTE2: this code also works OK on local dev machine WITHOUT HDFS
   *
   * @param srcPath : the source DIRECTORY containing all the part-xxxx files to be concatenated
   * @param dstFilename : the destination FILE (the one that concatenates all the part-xxxx
   */
  def combineNFilesTo1(srcPath: String, dstFilename: String): Unit =  {
    val hadoopConfig = new org.apache.hadoop.conf.Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstFilename), false, hadoopConfig, null)
  }

  /**
   * delete recursively a directory (Local or HDFS)
   * to avoid runtime error org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory file ...blabla... already exists
   *
   * @param dirtoDelete
   * @return
   */
  def deleteDirRecursive(dirtoDelete: String) = {
    printf("\tDeleting Directory [%s] ...\n", dirtoDelete)

    if (dirtoDelete.startsWith("hdfs")) {
      val hadoopConfig = new org.apache.hadoop.conf.Configuration()
      // Equivalent of
      // # hdfs dfs -rm -R -skipTrash <dirtoDelete>
      val dir2DelPath: Path = new Path(dirtoDelete)
      val hdfs: FileSystem = FileSystem.get(hadoopConfig)
      hdfs.delete(dir2DelPath, true)
    } else {
      // local FS
      // ThankYou: Dan Ciborowski @ http://stackoverflow.com/questions/25999255/delete-directory-recursively-in-scala
      org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(dirtoDelete))
    }
  }

  /**
   * Delete a single file (Local or HDFS)
   *
   * @param filetoDelete
   */
  def deleteFile(filetoDelete: String) = {
    printf("\tDeleting [%s] ...\n", filetoDelete)

    if (filetoDelete.startsWith("hdfs")) {
      val hadoopConfig = new org.apache.hadoop.conf.Configuration()
      // Equivalent of: hdfs dfs -rm -skipTrash <filetoDelete>
      val file2DelPath: Path = new Path(filetoDelete)
      val hdfs: FileSystem = FileSystem.get(hadoopConfig)
      hdfs.delete(file2DelPath, false)
    } else {
      val csvFile = new java.io.File(filetoDelete)
      csvFile.delete()
    }

  }

}
