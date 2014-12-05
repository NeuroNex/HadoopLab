import tradoop.OzoneAnalysis.HLYRecordParser;

import java.io.*;

/**
 * Quality Check the Ozone file (parse OK without runtime error?)
 * Results:
 * HLY File....: /home/tri/Documents/HadoopLab/AirQualityAnalysis/src/main/resources/2012O3.hly
 * Total Record: 74065
 * Valid Record: 72666
 * Short Record:     1 (have less than 125 chars)
 * Parse Error :     1
 * NULL found  :  1397
 *
 * 2014-04-19 - Tri Nguyen
 */
public class CheckOzoneFile {
	private static final String OZONE_LOCAL_FILENAME = "/home/tri/Documents/HadoopLab/AirQualityAnalysis/src/main/resources/2012O3.hly";
	private static final int MINIMUM_RECORD_LENGTH = 125;

	public static void main(String[] args) {
		BufferedReader buffReader = null;
		HLYRecordParser hlyParser = new HLYRecordParser();

		String currLine = "";
		int lineCount = 0, parseErrorCount = 0, NullFoundCount = 0, validRecordCount = 0;
		int shortRecordCount = 0;

		try {
			buffReader = new BufferedReader(new FileReader(OZONE_LOCAL_FILENAME));
			//buffReader = new BufferedReader(new InputStreamReader(new FileInputStream(OZONE_LOCAL_FILENAME)));
			while ((currLine = buffReader.readLine()) != null) {
				lineCount++;

				try {
					hlyParser.parse(currLine);

					if (currLine.length() < MINIMUM_RECORD_LENGTH) shortRecordCount++;

					if (hlyParser.isValidRecord()) validRecordCount++;

					if (hlyParser.getCalcDayAverage() == null || hlyParser.getCalcDayMin() == null || hlyParser.getCalcDayMax() == null) {
						NullFoundCount++;
					}
					//System.out.printf("%s -> %4d %4d %4d\n", currLine, Math.round(hlyParser.getCalcDayAverage()), hlyParser.getCalcDayMin(), hlyParser.getCalcDayMax());
				}
				catch (NullPointerException npex) {
					parseErrorCount++;
					//ignore, NULL is possible if the record had non numerical data
					System.out.printf("%s -> %s\n", currLine, npex.getClass().getSimpleName());
				}
				catch (Exception ex) {
					System.out.printf("FAILED at line %d\n%s\n\n", lineCount, currLine);
					ex.printStackTrace();
					break;
				}
				//if (lineCount > 20) break;
			}
			System.out.printf("HLY File....: %s\n", OZONE_LOCAL_FILENAME);
			System.out.printf("Total Record: %6d\n", lineCount);
			System.out.printf("Valid Record: %6d\n", validRecordCount);
			System.out.printf("Short Record: %6d (have less than %d chars)\n", shortRecordCount, MINIMUM_RECORD_LENGTH);
			System.out.printf("Parse Error : %6d\n", parseErrorCount);
			System.out.printf("NULL found  : %6d\n", NullFoundCount);
		}
		catch (Exception e) {
			System.out.printf("FAILED at line %d\n%s\n\n", lineCount, currLine);
			e.printStackTrace();
		}
		finally {
			try {
				if (buffReader != null) buffReader.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	/**
	 * Test Review various strategies to cleanup a string submitted to parseInt()
	 */
	private static String cleanupNum(String origTxt) {
/*
		String testNum = "   ++12++   ";
		System.out.printf("%s -> [%s]\n", testNum, cleanupNum(testNum));
		testNum = "   +V5M 3Z8+   ";
		System.out.printf("%s -> [%s]\n", testNum, cleanupNum(testNum));
		testNum = "   -3.14159   ";
		System.out.printf("%s -> [%s]\n", testNum, cleanupNum(testNum));
		System.out.printf("-> Float = %s\n", Float.parseFloat(cleanupNum(testNum)));
		System.out.printf("-> Int   = %d\n", safeParseInteger(testNum)));

		testNum = "HelloABC";
		System.out.printf("%s -> [%s]\n", testNum, cleanupNum(testNum));
		System.out.printf("-> Int   = %d\n", safeParseInteger(testNum)));

		testNum = "123-ABC";
		System.out.printf("%s -> [%s]\n", testNum, cleanupNum(testNum));
*/

		//Clean #1: Remove spaces and + symbole
		//Ex: " 12", "+12", " +12 ", "   +++12", "  ++12++  " all become "12"
		//return origTxt.replaceAll("[ \\+]+","");

		/*
		Clean #2: (more agressive) remove all EXCEPT: digits, minus symbol, period
		Example:
		"   ++12++   "   -> "12"
		"   +V5M 3Z8+  " -> "538"
		"   -3.14159   " -> "-3.14159]"
		"HelloABC"       -> ""
		"123-ABC"        -> "123-"
		*/
		return origTxt.replaceAll("[^\\d\\.\\-]","");
	}
}
