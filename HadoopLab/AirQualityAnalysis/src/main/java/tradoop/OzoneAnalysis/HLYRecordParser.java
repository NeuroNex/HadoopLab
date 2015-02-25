package tradoop.OzoneAnalysis;

/**
 * Parse a fixed length record of a *.hly file from
 * Canadian National Air Pollution Surveillance Program (NAPS)
 * http://maps-cartes.ec.gc.ca/rnspa-naps/data.aspx?lang=en
 * The file format is descibed in /src/main/resources/00_Readme.txt
 *
 *
 * 2014-04-18 - Tri Nguyen
 */
public class HLYRecordParser {
	private static final int INVALID_MEASURE = -999;
	private static final int MINIMUM_RECORD_LENGTH = 125;

	private String _strDate;
	private Integer _stationID, _dayAverage, _dayMin, _dayMax;
	private Integer[] _hourReadings = new Integer[24];
	private boolean _metRequiredLength = false;

	/**
	 * Parse a Fixed-Length NAPS HLY record
	 * NAPS: National Air Pollution Surveillance Program
	 * HLY : no idea but this is the file format of all NAPS measures: http://maps-cartes.ec.gc.ca/rnspa-naps/data.aspx?lang=en
	 */
	public void parse(String line) {

/*
PC Stat  YYYYMMDD AVG MIN MAX H01 H02 H03 H04 H05 H06 H07 H08 H09 H10 H11 H12 H13 H14 H15 H16 H17 H18 H19 H20 H21 H22 H23 H24
00701010220120105  23   5  29  26  25  25  28  28  27  29  28  26  26  26  27-999-999-999-999  23  26  24  22  16  10   8   5
00701010220120106  25  17  31  19  31  29  29  29  30  31  30  26  20  17-999  24  24  24  24  24  21  23  24  26  23  23  27
   ssssss

s: StationID

*/
		// an HLY record is always fixed length = 125 chars
		this._metRequiredLength = (line.length() >= MINIMUM_RECORD_LENGTH);

		if (this._metRequiredLength) {
			this._stationID = safeIntegerConverter(line.substring(3, 9));
			this._strDate = line.substring(9, 17);
			this._dayAverage = safeIntegerConverter(line.substring(17, 21));
			this._dayMin = safeIntegerConverter(line.substring(21, 25));
			this._dayMax = safeIntegerConverter(line.substring(25, 29));

			this._hourReadings[0] = safeIntegerConverter(line.substring(29, 33));
			this._hourReadings[1] = safeIntegerConverter(line.substring(33, 37));
			this._hourReadings[2] = safeIntegerConverter(line.substring(37, 41));
			this._hourReadings[3] = safeIntegerConverter(line.substring(41, 45));
			this._hourReadings[4] = safeIntegerConverter(line.substring(45, 49));
			this._hourReadings[5] = safeIntegerConverter(line.substring(49, 53));
			this._hourReadings[6] = safeIntegerConverter(line.substring(53, 57));
			this._hourReadings[7] = safeIntegerConverter(line.substring(57, 61));
			this._hourReadings[8] = safeIntegerConverter(line.substring(61, 65));
			this._hourReadings[9] = safeIntegerConverter(line.substring(65, 69));
			this._hourReadings[10] = safeIntegerConverter(line.substring(69, 73));
			this._hourReadings[11] = safeIntegerConverter(line.substring(73, 77));
			this._hourReadings[12] = safeIntegerConverter(line.substring(77, 81));
			this._hourReadings[13] = safeIntegerConverter(line.substring(81, 85));
			this._hourReadings[14] = safeIntegerConverter(line.substring(85, 89));
			this._hourReadings[15] = safeIntegerConverter(line.substring(89, 93));
			this._hourReadings[16] = safeIntegerConverter(line.substring(93, 97));
			this._hourReadings[17] = safeIntegerConverter(line.substring(97, 101));
			this._hourReadings[18] = safeIntegerConverter(line.substring(101, 105));
			this._hourReadings[19] = safeIntegerConverter(line.substring(105, 109));
			this._hourReadings[20] = safeIntegerConverter(line.substring(109, 113));
			this._hourReadings[21] = safeIntegerConverter(line.substring(113, 117));
			this._hourReadings[22] = safeIntegerConverter(line.substring(117, 121));
			this._hourReadings[23] = safeIntegerConverter(line.substring(121, 125));
		}
	}

	/**
	 * A Record is valid IF all these conditions are met:
	 * - Record length must meet minimum record length
	 * - At least 1 valid values among the 24 Hours Readings
	 *   (which means at least any og the Calculated Average, Min, Max must be not null)
	 */
	public boolean isValidRecord() {
		return (this._metRequiredLength && this._stationID != null && this.getCalcDayAverage() != null);
	}

	public Integer getStationID() {
		return this._stationID;
	}

	public String getDateString() {
		return this._strDate;
	}

	/**
	 * The PreCalculated AVERAGE measure value of the day
	 * Pre-Calc means the value was alread calculated by NAPS and written in the record
	 * NOTE: value = -999 should be considered as NULL (to be ignored)
	 */
	public Integer getDayAverage() {
		return (this._dayAverage == INVALID_MEASURE ? null : this._dayAverage);
	}

	/**
	 * The PreCalculated MINIMUM measure value of the day
	 * Pre-Calc means the value was alread calculated by NAPS and written in the record
	 * NOTE: value = -999 should be considered as NULL (to be ignored)
	 */
	public Integer getDayMin() {
		return (this._dayMin == INVALID_MEASURE ? null : this._dayMin);
	}

	/**
	 * The PreCalculated MAXIMUM measure value of the day
	 * Pre-Calc means the value was alread calculated by NAPS and written in the record
	 * NOTE: value = -999 should be considered as NULL (to be ignored)
	 */
	public Integer getDayMax() {
		return (this._dayMax == INVALID_MEASURE ? null : this._dayMax);
	}

	/**
	 * The AVERAGE measure, Calculated from the 24 hours measures
	 */
	public Float getCalcDayAverage() {
		int sum = 0, validMeasureCount = 0;
		for (int kk = 0; kk < this._hourReadings.length; kk++) {
			if (this._hourReadings[kk] != INVALID_MEASURE) {
				sum += this._hourReadings[kk];
				validMeasureCount++;
			}
		}

		if (validMeasureCount > 0) {
			// missing values must NOT participate in the average calculation
			// otherwise the null value will contribute to lower the average
			// For pollution measure for example, a station will 23 missing measures out of 24
			// will appear as very clean because its average will 1 value / 24 if null were counted in the denominator
			return (float) sum / validMeasureCount;
		}
		else
			return null;
	}

	/**
	 * The MINIMUM measure, Calculated from the 24 hours measures
	 */
	public Integer getCalcDayMin() {
		int min = Integer.MAX_VALUE, validMeasureCount = 0;
		for (int kk = 0; kk < this._hourReadings.length; kk++) {
			if (this._hourReadings[kk] != INVALID_MEASURE) {
				if (min > this._hourReadings[kk]) {
					min = this._hourReadings[kk];
				}
				validMeasureCount++;
			}
		}
		if (validMeasureCount > 0)
			return min;
		else
			return null;
	}

	/**
	 * The MAXIMUM measure, Calculated from the 24 hours measures
	 */
	public Integer getCalcDayMax() {
		int max = Integer.MIN_VALUE, validMeasureCount = 0;
		for (int kk = 0; kk < this._hourReadings.length; kk++) {
			if (this._hourReadings[kk] != INVALID_MEASURE) {
				if (max < this._hourReadings[kk]) {
					max = this._hourReadings[kk];
				}
				validMeasureCount++;
			}
		}
		if (validMeasureCount > 0)
			return max;
		else
			return null;
	}


	/**
	 * Cleanup string to make it friendly to Integer.parseInt()
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
		System.out.printf("-> Int   = %d\n", safeIntegerConverter(testNum)));

		testNum = "HelloABC";
		System.out.printf("%s -> [%s]\n", testNum, cleanupNum(testNum));
		System.out.printf("-> Int   = %d\n", safeIntegerConverter(testNum)));

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


	/**
	 * The sloppy Java JDK is too lazy to make elementaty test on a string before Integer.parseInt()
	 * Minor format like " 12", "+12", "-3.14" will raises java.lang.NumberFormatException
	 * Here a failure to parse will return null instead of raising runtime error
	 */
	private static Integer safeIntegerConverter(String candidateNumStr) {
		try {
			/*
			Remove all EXCEPT: digits, minus symbol, period
			"   ++12++   "   -> "12"
			"   +V5M 3Z8+  " -> "538"
			"   -3.14159   " -> "-3.14159]"
			"HelloABC"       -> ""
			"123-ABC"        -> "123-"
			*/
			//return Integer.parseInt(candidateNumStr.replaceAll("[^\\d\\.\\-]",""));

			// remove spaces and +
			return Integer.parseInt(candidateNumStr.replaceAll("[ \\+]+",""));
		}
		catch (NumberFormatException ex) {
			return null;
		}
	}
}
