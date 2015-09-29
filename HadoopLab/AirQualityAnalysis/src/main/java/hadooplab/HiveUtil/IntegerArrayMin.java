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
 * Java UDF for Hive to find the Min value of a Hive Array<int> data type
 *
 * 2014-04-16 - Tri.Nguyen
 */
public class IntegerArrayMin extends org.apache.hadoop.hive.ql.exec.UDF {
	// why org.apache.hadoop.io.IntWritable and not Java int?
	// b/c of object re-use, it is more economic in memory
	private IntWritable _minValue = new IntWritable();

	public IntWritable evaluate(ArrayList<Integer> myIntArray) {

		if (myIntArray == null || myIntArray.size() < 1) {
			return null;
		}

		int min = Integer.MAX_VALUE;
		int validValueCount = 0;
		for (Integer i : myIntArray) {
			if (i != null) {
				validValueCount++;
				if (min > i) {
					min = i;
				}
			}
		}

		if (validValueCount > 0) {
			_minValue.set(min);
			return _minValue;
		}
		else {
			return null;
		}

	}
}
