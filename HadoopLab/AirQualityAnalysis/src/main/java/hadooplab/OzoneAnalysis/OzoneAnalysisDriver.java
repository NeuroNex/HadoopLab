package hadooplab.OzoneAnalysis;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
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
   # hdfs dfs -mkdir -p /user/tri/AirAnalysis/
   # cd /root/HadoopExo/

   --Exec on HDFS (+ send StationInfo to DistribCache via cmdline)
   # yarn jar OzoneMR.jar hadooplab.OzoneAnalysis.OzoneAnalysisDriver -files /root/HadoopExo/Stations_v28012014.csv

   --Exec on HDFS (StationInfo added to DistribCache via API in MR Code)
   # yarn jar OzoneMR.jar hadooplab.OzoneAnalysis.OzoneAnalysisDriver

	--VERIF:
   # hdfs dfs -cat /user/tri/AirAnalysis/output2/part-r-00000
   # rm -f OzoneByCities_JavaMR.txt
   # hdfs dfs -copyToLocal /user/tri/AirAnalysis/output2/part-r-00000 ./OzoneByCities_JavaMR.txt
   $ scp root@hdpsbhv:/root/HadoopExo/OzoneByCities_JavaMR.txt ~/Documents/IntelliJProjects/BigDataLAB/13_AirQualityAnalysis/Results/
*/

/**
 * Chaining 2 MapReduce jobs to produce a report "Most polluted cities in Ozone"
 * See results: ~/Documents/IntelliJProjects/BigDataLAB/13_AirQualityAnalysis/Results/OzoneByCities_JavaMR.txt
 * The results shown in OzoneByCities_JavaMR.txt can ONLY be obtained by chaining TWO MR Jobs
 * It is IMPOSSIBLE to do that in 1 single MR job, even when using elaborate techniques such as
 * SecondarySort, custom writable class
 *
 * 2014-04-21 - Tri Nguyen
 */
public class OzoneAnalysisDriver extends Configured implements Tool {
	private static final String STATION_HDFS_FILENAME = "/user/tri/AirAnalysis/NAPSStation/Stations_v28012014.csv";
	//private static final String NAPS_OZONE_INPUTFILE = "/user/tri/MiniOzoneSample.txt"; // path is on HDFS
	private static final String NAPS_OZONE_INPUTFILE = "/user/tri/AirAnalysis/Ozone2012RawFL/2012O3.hly"; // path is on HDFS
	private static final String REDUCER_OUTPUT_DIR = "/user/tri/AirAnalysis/output"; // path is on HDFS
	private static final String REDUCER_OUTPUT_DIR2 = "/user/tri/AirAnalysis/output2"; // path is on HDFS

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		//------------------------------------------------
		// Job 1:
		// - Map Output : Location, AverageOzone
		// - Reducer Out: Location, Max(AverageOzone)
		// - Order by Location
		//------------------------------------------------

		Job job = org.apache.hadoop.mapreduce.Job.getInstance(getConf()); // getConf() comes from inherited parent class Configured
		job.setJobName("hadooplab.OzoneAnalysis.OzoneAnalysisDriver #1 (GrpBy Location)");
		job.setJarByClass(getClass());

		// Delete automatically the output dir if its already exists
		// Equivalent of:
		// # hdfs dfs -rm -R -skipTrash /user/tri/AirAnalysis/output/
		Path outputPath = new Path(args[1]); // output directory (contain reducer results)
		System.out.printf("===^^^ Delete Output Directory: %s ^^^===\n", outputPath.toString());
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), getConf());
		fs.delete(outputPath, true); // true=recursive

		FileInputFormat.addInputPath(job, new Path(args[0])); // input file for mapper
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setMapperClass(LocationMapper.class);
		job.setCombinerClass(MaxOzoneReducer.class);
		job.setReducerClass(MaxOzoneReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Send the Station Info to Hadoop Distributed Cache via API
		// equivalence of cmdline option -file hdfsFilename. Example:
		// # yarn jar OzoneMR.jar CompositeKeyDriver -files /root/HadoopExo/Hive/Data/Stations_v28012014.csv
		java.net.URI stationFileURI = java.net.URI.create(STATION_HDFS_FILENAME);
		job.addCacheFile(stationFileURI);

		//Create a symlink to make the file available in the task working dir
		//Ex: the file specified by the URI hdfs://namenode/foo/bar#MyFile is symlinked as MyFile in the task’s working directory
		//NOTE: Files added to the distributed cache using GenericOptionsParser are automatically symlinked
		//job.createSymlink(); // no needed in Hadoop 2.x

		job.waitForCompletion(true);

		//------------------------------------------------
		// Job 2: (re-emit Job1 output with a different sorting)
		// - Map output : MaxAvgOzone, Location (order by Location)
		// - Reducer Out: MaxAvgOzone, Location (order by MaxAvgOzone DESCENDING)
		//   NOTE: the reducer makes no calculation, it re-emit exactly the map input
		//------------------------------------------------

		Path outputPath2 = new Path(REDUCER_OUTPUT_DIR2); // output directory (contain reducer results)
		fs.delete(outputPath2, true); // true=recursive

		Job job2 = org.apache.hadoop.mapreduce.Job.getInstance(getConf()); // getConf() comes from inherited parent class Configured
		FileInputFormat.addInputPath(job2, outputPath);
		FileOutputFormat.setOutputPath(job2, outputPath2);
		job2.setJobName("hadooplab.OzoneAnalysis.OzoneAnalysisDriver #2 (Order by MaxAvgOzone)");
		job2.setJarByClass(getClass());
		job2.setMapperClass(IdentityMapper.class);
		job2.setReducerClass(IdentityReducer.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		job2.setSortComparatorClass(DescendingIntComparator.class);

		return job2.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Start Driver using Tool Runner allowing to pass args[] and custom Hadoop config and/or properties at the command line
	 *
	 * @param args arg1: HDFS path of the Ozone HLY File, arg2: HDFS path of directory where the Reducer will save output file
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new OzoneAnalysisDriver(), new String[] {NAPS_OZONE_INPUTFILE, REDUCER_OUTPUT_DIR});
		System.exit(exitCode);
	}


	public static class MaxOzoneReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		/**
		 * Simple Reducer computing the max value
		 *
		 * @param locationName : input key transmitted by the mapper
		 * @param ozoneValues : input values collection transmitted by the mapper, here a list of all the Average Ozone measures found for keyStationID
		 */
		@Override
		public void reduce(Text locationName, Iterable<IntWritable> ozoneValues, Context context)
				throws IOException, InterruptedException {

			int maxValue = Integer.MIN_VALUE;
			for (IntWritable value : ozoneValues) {
				maxValue = Math.max(maxValue, value.get());
			}
			context.write(locationName, new IntWritable(maxValue));
		}
	}

	/**
	 * Identity Mapper:
	 * Input : (LocationName, avgOzoneValue)
	 * Output: (avgOzoneValue, LocationName)
	 *
	 * 2014-04-20 - Tri Nguyen
	 */
	public static class IdentityMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				/*
				BRITISH COLUMBIA	METRO VAN-CHILLIWACK	28
				NUNAVUT	ALERT	32
				ONTARIO	ALGOMA	82
				 */
				String[] elems = value.toString().split("\\t", -1); // KEEP empty string between delimiters

				context.write(new IntWritable(Integer.parseInt(elems[2])), new Text(String.format("%s\t%s", elems[0], elems[1])));
			}
			catch (Exception ex) {
				// Display the error msg in the Console as a Map Status Msg, which will be displayed
				// among the flow of progression msg of the MR Job
				context.setStatus(String.format("FAILED Parsing Input Record, ERROR: %s, InputLength:%d, InputRecord: %s",
						ex.getMessage(), value.toString().length(), value.toString()));

				// Dynamic counter (better adapted for this exercise)
				// Syntax: context.getCounter(CounterGroupName, CounterName).increment(1);
				// In the example below we get:
				// CounterGroupName = "hadooplab.OzoneAnalysis.SimpleStationMapper"
				// CounterName = "StringIndexOutOfBoundsException"
				context.getCounter(getClass().getSimpleName(), ex.getClass().getSimpleName()).increment(1);
			}

		}
	}


	/**
	 * Reemit the same intput, the reducer here is juste used for sorting
	 */
	public static class IdentityReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		public void reduce(IntWritable maxAvgOzone, Iterable<Text> locationNames, Context context)
				throws IOException, InterruptedException {

			try {
				//context.write(locationNames.iterator().next(), maxAvgOzone);
				for (Text location : locationNames) {
					context.write(maxAvgOzone, location);
				}
			}
			catch (Exception ex) {
				// Display the error msg in the Console as a Map Status Msg, which will be displayed
				// among the flow of progression msg of the MR Job
				System.err.println(String.format("FAILED Reducer maxAvgOzone: %s, Location: %s, ERROR: %s", maxAvgOzone.toString(), locationNames.iterator().next().toString(), ex.getMessage()));

				// Dynamic counter (better adapted for this exercise)
				// Syntax: context.getCounter(CounterGroupName, CounterName).increment(1);
				// In the example below we get:
				// CounterGroupName = "CompositeKeyReducer"
				// CounterName = "StringIndexOutOfBoundsException"
				context.getCounter(getClass().getSimpleName(), ex.getClass().getSimpleName()).increment(1);
			}
		}
	}

	/**
	 * Descending Comparator for IntWritableto be used in driver program:
	 * Example job.setSortComparatorClass(DescendingIntComparator.class);
	 *
	 * Thank you: https://gist.github.com/geofferyzh/3839714
	 */
	public static class DescendingIntComparator extends WritableComparator {
		protected DescendingIntComparator() {
			super(IntWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntWritable k1 = (IntWritable)w1;
			IntWritable k2 = (IntWritable)w2;
			return -1 * k1.compareTo(k2);
		}
	}
}
