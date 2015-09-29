package hadooplab.HiveUtil;

/*
HOW TO TEST
Create JAR file
   Ctrl-Shift-F9 (compile the module)
   $ cd ~/Documents/IntelliJProjects/BigDataLAB/13_AirQualityAnalysis/target/classes/
   $ jar cfv MyHiveUDF.jar ./hadooplab.HiveUtil/*.class
   $ scp MyHiveUDF.jar root@hdpsbhv:/root/HadoopExo/Hive/

See how the UDF is used in ~/Documents/IntelliJProjects/BigDataLAB/13_AirQualityAnalysis/src/main/hive/2_Ozone2012.hiveql
*/

import org.apache.hadoop.io.IntWritable;
import java.util.ArrayList;

/**
 * Java UDF for Hive to find the Max value of a Hive Array<int> data type
 * Thank you: Lorand Bendig http://stackoverflow.com/questions/12380955/summing-values-of-hive-array-types
 *
 * 2014-04-16 - Tri.Nguyen
 */
public class IntegerArrayMax extends org.apache.hadoop.hive.ql.exec.UDF {
	// why org.apache.hadoop.io.IntWritable and not Java int?
	// b/c of object re-use, it is more economic in memory
	private IntWritable _maxValue = new IntWritable();

	public IntWritable evaluate(ArrayList<Integer> myIntArray) {

		if (myIntArray == null || myIntArray.size() < 1) {
			return null;
		}

		int max = Integer.MIN_VALUE;
		int validValueCount = 0;
		for (Integer i : myIntArray) {
			if (i != null) {
				validValueCount++;
				if (max < i) {
					max = i;
				}
			}
		}
		if (validValueCount > 0) {
			_maxValue.set(max);
			return _maxValue;
		}
		else {
			return null;
		}
	}
}
