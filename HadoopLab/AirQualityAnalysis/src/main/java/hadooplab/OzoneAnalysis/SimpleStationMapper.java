package hadooplab.OzoneAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


/**
 * Mapping: StationID -> Integer Average(of the 24 readings)
 *
 * PC Stat  YYYYMMDD AVG MIN MAX H01 H02 H03 H04 H05 H06 H07 H08 H09 H10 H11 H12 H13 H14 H15 H16 H17 H18 H19 H20 H21 H22 H23 H24
 * 00701010220120105  23   5  29  26  25  25  28  28  27  29  28  26  26  26  27-999-999-999-999  23  26  24  22  16  10   8   5
 * 00701010220120106  25  17  31  19  31  29  29  29  30  31  30  26  20  17-999  24  24  24  24  24  21  23  24  26  23  23  27
 *    ssssss
 * s: StationID
 *
 * 2014-04-19 - Tri Nguyen
 */
public class SimpleStationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private HLYRecordParser _hlyParser = new HLYRecordParser();

	/**
	 * mapping of an input fed by the default "TextInputFormat"
	 *
	 * @param key: the offset of the data record from the beginning of the data file (default from TextInputFormat<LongWritable, Text> set in Driver class)
	 * @param value: the line of text which is read by the FileInputFormat class responsible to feed the Mapper
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		this._hlyParser.parse(value.toString());

		try {

			if (this._hlyParser.isValidRecord()) {
				context.write(new Text(this._hlyParser.getStationID().toString()), new IntWritable( Math.round(this._hlyParser.getDayAverage())));
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
			// CounterGroupName = "hadooplab.OzoneAnalysis.SimpleStationMapper"
			// CounterName = "StringIndexOutOfBoundsException"
			context.getCounter(getClass().getSimpleName(), ex.getClass().getSimpleName()).increment(1);
		}
	}
}
