package hadooplab.HiveUtil;

/*
HOW TO TEST
Create JAR file
   Ctrl-Shift-F9 (compile the module)
   $ cd ~/Documents/IntelliJProjects/BigDataLAB/13_AirQualityAnalysis/target/classes/
   $ jar cfv MyHiveUDF.jar ./hadooplab/HiveUtil/*.class
   $ scp MyHiveUDF.jar root@hdpsbhv:/root/HadoopExo/Hive/

See how the UDF is used in ~/Documents/IntelliJProjects/BigDataLAB/13_AirQualityAnalysis/src/main/hive/2_Ozone2012.hiveql
*/

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import java.util.ArrayList;

/**
 * Java UDF for Hive to find the Average value of a Hive Array<int> data type
 *
 * 2014-04-16 - Tri.Nguyen
 */
public class IntegerArrayAvg extends org.apache.hadoop.hive.ql.exec.UDF {
	// why org.apache.hadoop.io.DoubleWritable and not Java int?
	// b/c of object re-use, it is more economic in memory
	private DoubleWritable _avgValue = new DoubleWritable();

	public DoubleWritable evaluate(ArrayList<Integer> myIntArray) {

		if (myIntArray == null || myIntArray.size() < 1) {
			return null;
		}

		double sum = 0;
		int validValueCount = 0;
		for (Integer i : myIntArray) {
			if (i != null) {
				sum += i;
				validValueCount++;
			}
		}

		_avgValue.set(sum / validValueCount);

		// unfair: missing value contribute to lower average
		// For pollution measure for example, a station full of missing measures appear as very clean
		//_avgValue.set(sum / myIntArray.size());
		return _avgValue;
	}
}
