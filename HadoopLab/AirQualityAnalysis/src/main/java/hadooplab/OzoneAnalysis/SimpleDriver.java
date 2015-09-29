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
   $ jar cfv OzoneMR.jar ./*.class
   $ scp OzoneMR.jar root@hdpsbhv:/root/HadoopExo/

Data Preparation (USING _SAME_ data files than HIVE, on HDFS)
   $ scp ~/Documents/IntelliJProjects/BigDataLAB/13_AirQualityAnalysis/src/main/resources/Stations_v28012014.csv root@hdpsbhv:/root/HadoopExo/Hive/Data/
   $ scp ~/Documents/IntelliJProjects/BigDataLAB/13_AirQualityAnalysis/src/main/resources/MiniOzoneSample.txt root@hdpsbhv:/root/HadoopExo/Hive/Data/
   # hdfs dfs -mkdir -p /user/tri/AirAnalysis/NAPSStation/
   # hdfs dfs -copyFromLocal -f /root/HadoopExo/Hive/Data/Stations_v28012014.csv /user/tri/AirAnalysis/NAPSStation/
   # hdfs dfs -copyFromLocal -f /root/HadoopExo/Hive/Data/MiniOzoneSample.txt /user/tri/

   $ scp ~/Documents/IntelliJProjects/BigDataLAB/13_AirQualityAnalysis/src/main/resources/2012O3.hly root@hdpsbhv:/root/HadoopExo/Hive/Data/
   # hdfs dfs -mkdir -p /user/tri/AirAnalysis/Ozone2012RawFL/
   # hdfs dfs -copyFromLocal -f ./Data/2012O3.hly /user/tri/AirAnalysis/Ozone2012RawFL/

Run the Java Program (ssh and run on Hadoop node)
   # hdfs dfs -mkdir -p /user/tri/AirAnalysis/
   # cd /root/HadoopExo/

  --Exec on HDFS (StationInfo added to DistribCache via API in MR Code)
   # yarn jar OzoneMR.jar hadooplab.OzoneAnalysis.SimpleDriver

	--VERIF:
   # hdfs dfs -cat /user/tri/AirAnalysis/output/part-r-00000
   # hdfs dfs -copyToLocal /user/tri/AirAnalysis/output/part-r-00000 ./OzoneByCities_JavaMR.txt
   $ scp root@hdpsbhv:/root/HadoopExo/OzoneByCities_JavaMR.txt ~/Documents/IntelliJProjects/BigDataLAB/13_AirQualityAnalysis/Results/
*/

/**
 * Driver program (the simplest implementation)
 * to start the MapReduce processing of the NAPS Ozone2012 records
 * The end results is an output file (StationID, MaxCalcAverageOzone) sorted by StationID ascending
 *
 * Example:
 * 010102   45
 * 010401   51
 * 010501   51
 * 010601   46
 * 010602   57
 * 010801   56
 * 030113   16
 * 030118   35
 * 030120   46
 * 030201   47
 * .... (250 lines in total)
 *
 * 2014-04-19 - Tri Nguyen
 */
public class SimpleDriver extends Configured implements Tool {
	private static final String NAPS_OZONE_INPUTFILE = "/user/tri/MiniOzoneSample.txt"; // path is on HDFS
	//private static final String NAPS_OZONE_INPUTFILE = "/user/tri/AirAnalysis/Ozone2012RawFL/2012O3.hly"; // path is on HDFS
	private static final String REDUCER_OUTPUT_DIR = "/user/tri/AirAnalysis/output"; // path is on HDFS

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = org.apache.hadoop.mapreduce.Job.getInstance(getConf()); // getConf() comes from inherited parent class Configured
		job.setJobName("NAPS (StationID, AvgOzone)");
		job.setJarByClass(getClass());

		// Delete automatically the output dir if its already exists
		// Equivalent of:
		// # hdfs dfs -rm -R -skipTrash /user/tri/AirAnalysis/output/
		System.out.printf("Delete Output Directory: %s\n", args[1]);
		Path outputPath = new Path(args[1]); // output directory (contain reducer results)
		FileSystem fs = FileSystem.get(new URI(args[1]), getConf());
		fs.delete(outputPath, true); // true=recursive

		FileInputFormat.addInputPath(job, new Path(args[0])); // input file for mapper
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(SimpleStationMapper.class);
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(IntWritable.class);

		// The "simple" Reducer just computes max() which is an associative operator, therefore compatible as Combiner
		job.setCombinerClass(SimpleMaxAvgReducer.class);
		job.setReducerClass(SimpleMaxAvgReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Start Driver using Tool Runner allowing to pass args[] and custom Hadoop config and/or properties at the command line
	 *
	 * @param args arg1: HDFS path of the Ozone HLY File, arg2: HDFS path of directory where the Reducer will save output file
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SimpleDriver(), new String[] {NAPS_OZONE_INPUTFILE, REDUCER_OUTPUT_DIR});
		System.exit(exitCode);
	}

}
