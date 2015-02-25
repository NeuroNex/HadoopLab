package tradoop.OzoneAnalysis;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

/**
 * Mapping: outputing (Location, AvgOzone Value).
 *
 * + Perform a Map-side JOIN to substitute StationID by "Province City"
 *   the map side join uses Station Info local file available via Distributed Cache
 *
 * + Manage Custom Counter to hint about failed records
 *
 * NOTE: the JOIN must be at the MAP side so that the Location Keys will be "GROUP BY" by the reducer
 * if the JOIN was made at Reducer side, there will be duplicated Location in the results
 * b/c mapper emits StationID as key. The reducer will just substitute StationID by Location
 * and as there are often more than one station in a city, there will be duplicates on Location.
 *
 * 2014-04-20 - Tri Nguyen
 */
public class LocationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final String STATION_LOCAL_RELATIVE_FILENAME = "Stations_v28012014.csv";
	private HLYRecordParser _hlyParser = new HLYRecordParser();
	private NAPSStationLookup _stationLookup;

	// Example where this Enum is fully used: htdg.Ch05_MRDev.MaxTempMapperRobust
	enum NAPS_HLYRecordQuality {
		INVALID, // record too short, unreadable or contained corrupted data
		MissingLocation // cannot resolve the LocationName via the StationID
	}

	/**
	 * Retrieve the cache file using its original name, relative to the working directory of the task.
	 * The cache file is automatically copied from Hadoop's Distributed Cache to each Task node.
	 * Therefore, the reducer justs needs to assume that the file is available locally
	 * in the same directory than the JAR contaning the Reducer code
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		_stationLookup = new NAPSStationLookup();
		// Version1: The Station Info file was sent to the Distributed Cache via the cmdline using GenericOptionsParser (-file argument)
		// # yarn jar OzoneMR.jar tradoop.OzoneAnalysis.LocationDriver -files /root/HadoopExo/Stations_v28012014.csv
		//_stationLookup.initialize(new File(STATION_LOCAL_RELATIVE_FILENAME));

		// Version2: The Station Info file was added by code (in the Driver class) using API (hardcoded, no cmdline)
		// NOTE: context.getLocalCacheFiles() is deprecated in Hadoop 2.x
//		Path[] localPaths = context.getLocalCacheFiles();
//		if (localPaths.length == 0) {
//			throw new FileNotFoundException(String.format("Distributed cache file for NAPS StationInfo not found: %s", STATION_LOCAL_RELATIVE_FILENAME));
//		}
//		File stationInfoLocalFile = new File(localPaths[0].toString());

		// Version 3: same as 2, using Hadoop 2.x syntax
		// The Station Info file was added by code (in the Driver class) using API (hardcoded, no cmdline)
		URI[] dcFileURIs = context.getCacheFiles();
		if (dcFileURIs.length == 0) {
			throw new FileNotFoundException(String.format("Distributed cache file for NAPS StationInfo not found: %s", STATION_LOCAL_RELATIVE_FILENAME));
		}
		//Error: java.io.FileNotFoundException: /user/Tri/AirAnalysis/NAPSStation/Stations_v28012014.csv (No such file or directory)
		//File stationInfoLocalFile = new File(dcFileURIs[0].getPath());

		// When Hadoop upload a file to the Distibuted Cache, the file is avail. in the working folder of the map/reduce task
		// Hadoop 2.x: by a strange design decision, the dist-cached file has it's original absolute URI
		// Example:
		// dcFileURIs[0].getPath() = /user/Tri/AirAnalysis/NAPSStation/Stations_v28012014.csv
		// Must strip the parent path and only take the filename
		String localStationFileName = FilenameUtils.getName(dcFileURIs[0].getPath()); // "Stations_v28012014.csv"
		File stationInfoLocalFile = new File(localStationFileName);

		_stationLookup.initialize(stationInfoLocalFile);
	}

	/**
	 * Mapping of an input fed by the default "TextInputFormat"
	 * 1. Parse a Ozone record from the NAPS HLY File to get StationID, Ozone value
	 * 2. Lookup the Station Info in Distributed Cached which was loaded locally in the working folder of the task  by YARN
	 * 3. Match StationID to get Location (Province CityName)
	 * 4. Emit output record (Location, OzoneValue)
	 *
	 * @param key: the offset of the data record from the beginning of the data file (default from TextInputFormat<LongWritable, Text> set in Driver class)
	 * @param value: the line of text which is read by the FileInputFormat class responsible to feed the Mapper
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		try {
			this._hlyParser.parse(value.toString());

			if (this._hlyParser.isValidRecord()) {
				String stationLocation = _stationLookup.getStationLocation(this._hlyParser.getStationID());

				if (stationLocation == null) {
					context.getCounter(NAPS_HLYRecordQuality.MissingLocation).increment(1);
				} else {
					int avgOzone = Math.round(this._hlyParser.getDayAverage());
					context.write(new Text(stationLocation), new IntWritable(avgOzone));
					//System.out.printf("===^^^===%s, stationLocation: %s, avgOzone:%d\n", getClass().getSimpleName(), stationLocation, avgOzone);
				}
			}
			else {
				context.getCounter(NAPS_HLYRecordQuality.INVALID).increment(1);
			}
		}
		catch (Exception ex) {
			// Display the error msg in the Console as a Map Status Msg, which will be displayed
			// among the flow of progression msg of the MR Job
			context.setStatus(String.format("FAILED Parsing Input Record, ERROR: %s, InputLength:%d, InputRecord: %s",
					ex.getMessage(), value.toString().length(), value.toString()));

			// Dynamic counter (better adapted for this exercise)
			// Syntax: context.getCounter(CounterGroupName, CounterName).increment(1);
			// In the example below we get:
			// CounterGroupName = "tradoop.OzoneAnalysis.SimpleStationMapper"
			// CounterName = "StringIndexOutOfBoundsException"
			context.getCounter(getClass().getSimpleName(), ex.getClass().getSimpleName()).increment(1);
		}
	}
}
