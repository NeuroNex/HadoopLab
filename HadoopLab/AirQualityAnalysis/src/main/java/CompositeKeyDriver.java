import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/*
HOW TO TEST:

Deploy JAR file
   $ cd ~/Documents/IntelliJProjects/BigDataLAB/13_AirQualityAnalysis/target/classes/
   $ jar cfv OzoneAnalysisJavaMR.jar ./*.class
   $ scp OzoneAnalysisJavaMR.jar root@hdpsbhv:/root/HadoopExo/

Data Setup:
   Read section "Data Setup" in 13_AirQualityAnalysis/src/main/hive/README.md

Run the Java Program (ssh and run on Hadoop node)
   # hdfs dfs -mkdir -p /user/tri/OzoneJavaMR/
   # cd /root/HadoopExo/

   --Delete Output Folder if already exists
   # hdfs dfs -rm -R -skipTrash /user/tri/OzoneJavaMR/output/

   --Exec on HDFS (+ send StationInfo to Distributed Cache via cmdline)
   # hadoop jar OzoneAnalysisJavaMR.jar CompositeKeyDriver -files /root/HadoopExo/Hive/Data/Stations_v28012014.csv

   --Exec on HDFS (StationInfo added to DistribCache via API in MR Code)
   # hadoop jar OzoneAnalysisJavaMR.jar CompositeKeyDriver

	--VERIF:
   # hdfs dfs -cat /user/tri/OzoneJavaMR/output/part-r-00000
   # rm -f ./OzoneByCities_JavaMR.txt
   # hdfs dfs -copyToLocal /user/tri/OzoneJavaMR/output/part-r-00000 ./OzoneByCities_JavaMR.txt
   $ scp root@hdpsbhv:/root/HadoopExo/OzoneByCities_JavaMR.txt ~/Documents/IntelliJProjects/BigDataLAB/13_OzoneAnalysisJavaMR/Results/
*/

/**
 * Driver program to start the MapReduce processing of the NAPS Ozone2012 records
 * + Secondary Sort to sort the results by the Value (the Max of Average Ozone measures)
 *
 * This Driver makes use of advanced concepts:
 * - Distribute Cache API
 * - custom Partitioner, GroupComparer, SortComparator
 * - A Mapper which implements Map-Side join
 * - Delete automatically the Reduce Output Dir if it exists, which unfortunately is NOT working
 *
 * Example:
 * .... (250 lines in total)
 *
 * 2014-04-19 - Tri.Nguyen
 */
public class CompositeKeyDriver extends Configured implements Tool {
	private static final String STATION_HDFS_FILENAME = "/user/tri/OzoneJavaMR/NAPSStation/Stations_v28012014.csv";
	//private static final String NAPS_OZONE_INPUTFILE = "/user/tri/MiniOzoneSample.txt"; // path is on HDFS
	private static final String NAPS_OZONE_INPUTFILE = "/user/tri/OzoneJavaMR/Ozone2012RawFL/2012O3.hly"; // path is on HDFS
	private static final String REDUCER_OUTPUT_DIR = "/user/tri/OzoneJavaMR/output"; // path is on HDFS

	//@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = org.apache.hadoop.mapreduce.Job.getInstance(getConf()); // getConf() comes from inherited parent class Configured
		job.setJobName("Most Polluted Cities by Ozone");
		job.setJarByClass(getClass());

		// Delete automatically the output dir if its already exists
		// Equivalent of doing manually:
		// # hdfs dfs -rm -R -skipTrash /user/tri/OzoneJavaMR/output/
		System.out.printf("Delete Output Directory: %s\n", args[1]);
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(new URI(args[1]), getConf());
		fs.delete(outputPath, true); // true=recursive

		FileInputFormat.addInputPath(job, new Path(args[0])); // input file for mapper
		FileOutputFormat.setOutputPath(job, outputPath); // output directory (contain reducer results)

		job.setMapperClass(CompositeKeyMapper.class);
		job.setMapOutputKeyClass(LocationMeasureCompoKey.class);
		job.setMapOutputValueClass(NullWritable.class);

		// The "simple" Reducer just computes max() which is an associative operator, therefore compatible as Combiner
		//job.setCombinerClass(SimpleMaxAvgReducer.class);

		job.setReducerClass(CompositeKeyReducer.class);
		job.setOutputKeyClass(LocationMeasureCompoKey.class);
		job.setOutputValueClass(NullWritable.class);

		// Send the Station Info to Hadoop Distributed Cache via API
		// equivalence of cmdline option -file hdfsFilename. Example:
		// # hadoop jar OzoneAnalysisJavaMR.jar CompositeKeyDriver -files /root/HadoopExo/Hive/Data/Stations_v28012014.csv
		java.net.URI stationFileURI = java.net.URI.create(STATION_HDFS_FILENAME);
		job.addCacheFile(stationFileURI);

		//Create a symlink to make the file available in the task working dir
		//Ex: the file specified by the URI hdfs://namenode/foo/bar#MyFile is symlinked as MyFile in the task’s working directory
		//NOTE: Files added to the distributed cache using GenericOptionsParser are automatically symlinked
		job.createSymlink();

		//Settings for Secondary Sort (to sort Reducer reesults by value instead of by Key)
		job.setPartitionerClass(LocationPartitioner.class);
		job.setSortComparatorClass(LocAvgKeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Start Driver using Tool Runner allowing to pass args[] and custom Hadoop config and/or properties at the command line
	 *
	 * @param args arg1: HDFS path of the Ozone HLY File, arg2: HDFS path of directory where the Reducer will save output file
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CompositeKeyDriver(), new String[] {NAPS_OZONE_INPUTFILE, REDUCER_OUTPUT_DIR});
		System.exit(exitCode);
	}



	/**
	 * Custom SORT Comparator to sort the composite keys by all of its components:
	 * Compare1 LocationName (the grouping key)
	 * Compare2 AverageOzoneValue (descending), the value within the grouping key
	 * This sorting is used by the shuffle on the output of the map
	 * NOTE: this class is used in  job.setSortComparatorClass()
	 */
	public static class LocAvgKeyComparator extends WritableComparator {
		protected LocAvgKeyComparator() {
			super(LocationMeasureCompoKey.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			LocationMeasureCompoKey lmck1 = (LocationMeasureCompoKey) w1;
			LocationMeasureCompoKey lmck2 = (LocationMeasureCompoKey) w2;

			//==1==: Compare the grouping Key (Location)
			int compare = lmck1.getLocation().compareTo(lmck2.getLocation());
			if (compare == 0) {
				//==2== if group key was identical, Compare the value
				// Descending Order = reverse the "normal" compare result
				return LocationMeasureCompoKey.compareIntDescending(lmck1.getAverageOzone(), lmck2.getAverageOzone());
			}

			return compare;
		}
	}


	/**
	 * Custom partitioner to PARTITION by the Location field of the composite key
	 * This class garantees that all records belong to the same Location are stored
	 * in the same partition which will be used as input to the same reducer
	 * NOTE: this class is used in job.setPartitionerClass()
	 */
	public static class LocationPartitioner extends Partitioner<LocationMeasureCompoKey, NullWritable> {
		@Override
		public int getPartition(LocationMeasureCompoKey key, NullWritable value, int numReduceTasks) {
			int partitionNum = Math.abs(key.getLocation().hashCode()) % numReduceTasks;

			//System.out.printf("===^^^===Partitioner, Location:%s, ParitionNumber: %d\n", key.getLocation(), partitionNum);
			return partitionNum;
		}
	}


	/**
	 * Custom GROUP Comparator to group keys by the Location field of the composite key
	 * This class change the grouping of the keys in the reducer
	 * Instead of group by (Location, AverageOzone), this becomes group by (Location)
	 * The 2nd field of the composite key is IGNORED, so the entire reducer input
	 * is 1 single group of Location. As the reducer is fed from a partition which already isolated
	 * all records from the same Location. The reducer therefore "sees" one single key which is the Location
	 *
	 * ATTENTION: this grouping also takes care of sorting the AverageOzone value in descending order
	 * thanks to the implmentation of LocationMeasureCompoKey.compareTo()
	 *
	 * NOTE: this class is used in  job.setGroupingComparatorClass()
	 */
	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(LocationMeasureCompoKey.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			LocationMeasureCompoKey lmck1 = (LocationMeasureCompoKey) w1;
			LocationMeasureCompoKey lmck2 = (LocationMeasureCompoKey) w2;

			return lmck1.getLocation().compareTo(lmck2.getLocation());
		}

	}

}
