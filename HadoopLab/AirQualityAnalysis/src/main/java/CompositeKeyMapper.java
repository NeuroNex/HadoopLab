import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import hadooplab.OzoneAnalysis.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Mapping: outputing (compositeKey, Null). The compositeKey contains (Location, AverageOzoneValue)
 * see map() for more details about the mapping business logic
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
 * 2014-04-20 - Tri.Nguyen
 */
public class CompositeKeyMapper extends Mapper<LongWritable, Text, LocationMeasureCompoKey, NullWritable> {
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
		// # hadoop jar TorPASSApril2014.jar CompositeKeyDriver -files /root/HadoopExo/Hive/Data/Stations_v28012014.csv
		//_stationLookup.initialize(new File(STATION_LOCAL_RELATIVE_FILENAME));

		// Version2: The Station Info file was added by code (in the Driver class) using API (hardcoded, no cmdline)
		Path[] localPaths = context.getLocalCacheFiles();
		if (localPaths.length == 0) {
			throw new FileNotFoundException(String.format("Distributed cache file for NAPS StationInfo not found: %s", STATION_LOCAL_RELATIVE_FILENAME));
		}

		File stationInfoLocalFile = new File(localPaths[0].toString());
		_stationLookup.initialize(stationInfoLocalFile);
	}

	/**
	 * Mapping of an input fed by the default "TextInputFormat"
	 * The emit an output key of type LocationMeasureCompoKey, value is set to Null as it is not needed by the Reducer
	 * Reason: he composite key already contains everything the reducer needs to produce the final result
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
					int avgOzone = Math.round(this._hlyParser.getCalcDayAverage());
					LocationMeasureCompoKey compositeKey = new LocationMeasureCompoKey(stationLocation, avgOzone);
					context.write(compositeKey, NullWritable.get());
					//context.write(compositeKey, new IntWritable(avgOzone));
					//System.out.printf("===^^^===Mapper, CompositeKey: %s\n", compositeKey.toString());
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
			// CounterGroupName = "SimpleStationMapper"
			// CounterName = "StringIndexOutOfBoundsException"
			context.getCounter(getClass().getSimpleName(), ex.getClass().getSimpleName()).increment(1);
		}
	}
}
