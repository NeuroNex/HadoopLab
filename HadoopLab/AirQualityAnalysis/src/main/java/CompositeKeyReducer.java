import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Reducer outputing outputing (compositeKey, Null). The compositeKey contains (Location, AverageOzoneValue)
 * NOTE: this Reducer makes NO calculation of max(AverageOzoneValue)
 * The magic happens in the custom Partitioner, GroupComparer, SortComparator configured in the Driver class
 *
 * 2014-04-20 - Tri.Nguyen
 */
public class CompositeKeyReducer extends Reducer<LocationMeasureCompoKey, NullWritable, LocationMeasureCompoKey, NullWritable> {

	/**
	 * Simple Reducer + just an excep handler to improve robustness
	 *
	 * @param compoKey : input key transmitted by the mapper
	 * @param values :
	 */
	@Override
	public void reduce(LocationMeasureCompoKey compoKey, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {

		try {
			//System.out.printf("===^^^===Reducer, key: %s\n", compoKey.toString());

//			int maxValue = Integer.MIN_VALUE;
//			for (IntWritable value : values) {
//				maxValue = Math.max(maxValue, value.get());
//			}
//			context.write(compoKey, new IntWritable(maxValue));
			context.write(compoKey, NullWritable.get());
		}
		catch (Exception ex) {
			// Display the error msg in the Console as a Map Status Msg, which will be displayed
			// among the flow of progression msg of the MR Job
			context.setStatus(String.format("FAILED Reducer for compoKey: %s, ERROR: %s", compoKey.toString(), ex.getMessage()));

			// Dynamic counter (better adapted for this exercise)
			// Syntax: context.getCounter(CounterGroupName, CounterName).increment(1);
			// In the example below we get:
			// CounterGroupName = "CompositeKeyReducer"
			// CounterName = "StringIndexOutOfBoundsException"
			context.getCounter(getClass().getSimpleName(), ex.getClass().getSimpleName()).increment(1);
		}
	}


}
