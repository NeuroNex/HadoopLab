package hadooplab.OzoneAnalysis;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/*
HOW TO TEST:

Deploy JAR file
   $ cd ~/Documents/IntelliJProjects/BigDataLAB/13_AirQualityAnalysis/target/classes/
   $ jar cfv OzoneMR.jar ./hadooplab/OzoneAnalysis/*.class
   $ scp OzoneMR.jar root@hdpsbhv:/root/HadoopExo/

Data Preparation (USING _SAME_ data files than PIG & HIVE, on HDFS)
   $ scp ~/Documents/IntelliJProjects/BigDataLAB/13_AirQualityAnalysis/src/main/resources/Stations_v28012014.csv root@hdpsbhv:/root/HadoopExo/
   # hdfs dfs -mkdir -p /user/tri/AirAnalysis/NAPSStation/
   # hdfs dfs -copyFromLocal -f /root/HadoopExo/Stations_v28012014.csv /user/tri/AirAnalysis/NAPSStation/

   $ scp ~/Documents/IntelliJProjects/BigDataLAB/13_AirQualityAnalysis/src/main/resources/2012O3.hly root@hdpsbhv:/root/HadoopExo/
   # hdfs dfs -mkdir -p /user/tri/AirAnalysis/Ozone2012RawFL/
   # hdfs dfs -copyFromLocal -f /root/HadoopExo/2012O3.hly /user/tri/AirAnalysis/Ozone2012RawFL/

Run the Java Program (ssh and run on Hadoop node)
   # cd /root/HadoopExo/

   --Exec on HDFS (+ send StationInfo to DistribCache via cmdline)
   # yarn jar OzoneMR.jar hadooplab.OzoneAnalysis.LocationDriver -files /root/HadoopExo/Stations_v28012014.csv

   --Exec on HDFS (StationInfo added to DistribCache via API in MR Code)
   # yarn jar OzoneMR.jar hadooplab.OzoneAnalysis.LocationDriver

	--VERIF:
   # hdfs dfs -cat /user/tri/AirAnalysis/output/part-r-00000
   # hdfs dfs -copyToLocal /user/tri/AirAnalysis/output/part-r-00000 ./OzoneByCities_JavaMR.txt
   $ scp root@hdpsbhv:/root/HadoopExo/OzoneByCities_JavaMR.txt ~/Documents/IntelliJProjects/BigDataLAB/13_AirQualityAnalysis/Results/
*/

/**
 * Driver program to start the MapReduce processing of the NAPS Ozone2012 records
 * + Add Station Info in Distributed Cache
 * The end results is an output file (Location, MaxCalcAverageOzone) sorted by Location ascending
 *
 * Example:
 *
 ALBERTA	ESthER	21
 ALBERTA	GRANDE PRAIRIE	25
 ALBERTA	HIGHTOWER RIDGE	55
 BRITISH COLUMBIA	Courtenay	32
 BRITISH COLUMBIA	METRO VAN - COQUITLAM	16
 * ....
 BRITISH COLUMBIA	WHISTLER	28
 BRITISH COLUMBIA	WILLIAMS LAKE	28
 NEWFOUNDLAND AND LABRADOR	ST. JOHN'S	24
 NORTHWEST TERRITORIES	SNARE RAPIDS	31
 NUNAVUT	ALERT	32
 ONTARIO	ALGOMA	82
 ONTARIO	BRANTFORD	39
 ONTARIO	EGBERT	33
 ONTARIO	TORONTO	25
 QUEBEC	SAINT-FAUSTIN-LAC-CARRE	32
 QUEBEC	SUTTON	41
 SASKATCHEWAN	SASKATOON	29
 *
 * 2014-04-19 - Tri Nguyen
 */
public class LocationDriver extends Configured implements Tool {
	private static final String STATION_HDFS_FILENAME = "/user/tri/AirAnalysis/NAPSStation/Stations_v28012014.csv";
	//private static final String NAPS_OZONE_INPUTFILE = "/user/tri/MiniOzoneSample.txt"; // path is on HDFS
	private static final String NAPS_OZONE_INPUTFILE = "/user/tri/AirAnalysis/Ozone2012RawFL/2012O3.hly"; // path is on HDFS
	private static final String REDUCER_OUTPUT_DIR = "/user/tri/AirAnalysis/output"; // path is on HDFS

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = org.apache.hadoop.mapreduce.Job.getInstance(getConf()); // getConf() comes from inherited parent class Configured
		job.setJobName("hadooplab.OzoneAnalysis.LocationDriver");
		job.setJarByClass(getClass());

		// Delete automatically the output dir if its already exists
		// Equivalent of:
		// # hdfs dfs -rm -R -skipTrash /user/tri/AirAnalysis/output/
		Path outputPath = new Path(args[1]); // output directory (contain reducer results)
		System.out.printf("===^^^ Delete Output Directory: %s ^^^===\n", outputPath.toString());
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), getConf());
		fs.delete(outputPath, true); // true=recursive

		FileInputFormat.addInputPath(job, new Path(args[0])); // input file for mapper
		FileOutputFormat.setOutputPath(job, outputPath); // output directory (contain reducer results)

		job.setMapperClass(LocationMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// The "simple" Reducer just computes max() which is an associative operator, therefore compatible as Combiner
		job.setCombinerClass(SimpleMaxAvgReducer.class);
		job.setReducerClass(SimpleMaxAvgReducer.class);


		// Send the Station Info to Hadoop Distributed Cache via API
		// equivalence of cmdline option -file hdfsFilename. Example:
		// # yarn jar OzoneMR.jar CompositeKeyDriver -files /root/HadoopExo/Hive/Data/Stations_v28012014.csv
		java.net.URI stationFileURI = java.net.URI.create(STATION_HDFS_FILENAME);
		job.addCacheFile(stationFileURI);

		//Create a symlink to make the file available in the task working dir
		//Ex: the file specified by the URI hdfs://namenode/foo/bar#MyFile is symlinked as MyFile in the task’s working directory
		//NOTE: Files added to the distributed cache using GenericOptionsParser are automatically symlinked
		//job.createSymlink(); // no needed in Hadoop 2.x

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Start Driver using Tool Runner allowing to pass args[] and custom Hadoop config and/or properties at the command line
	 *
	 * @param args arg1: HDFS path of the Ozone HLY File, arg2: HDFS path of directory where the Reducer will save output file
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new LocationDriver(), new String[] {NAPS_OZONE_INPUTFILE, REDUCER_OUTPUT_DIR});
		System.exit(exitCode);
	}

}
