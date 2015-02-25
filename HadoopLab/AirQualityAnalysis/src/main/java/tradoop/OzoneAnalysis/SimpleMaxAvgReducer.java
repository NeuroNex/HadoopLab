package tradoop.OzoneAnalysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * The simplest Reducer: re-emit the mapper key + the max of all values from that key
 *
 * Input : AnyTextKey, (avg1, avg2, avg3)
 * Output: AnyTextKey, maxAverage
 *
 * 2014-04-19 - Tri Nguyen
 */
public class SimpleMaxAvgReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	/**
	 * Simple Reducer computing the max value
	 *
	 * @param keyStationID : input key transmitted by the mapper
	 * @param values : input values collection transmitted by the mapper, here a list of all the Average Ozone measures found for keyStationID
	 */
	@Override
	public void reduce(Text keyStationID, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}
		context.write(keyStationID, new IntWritable(maxValue));
	}
}
