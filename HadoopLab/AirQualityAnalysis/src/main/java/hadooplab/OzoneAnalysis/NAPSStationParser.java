package hadooplab.OzoneAnalysis;

/**
 * Parse the CSV File containing the NAPS Station Properties
 * Here we are only interested by: StationID, ProvinceName, City
 *
 * 2014-04-19 - Tri Nguyen
 */
public class NAPSStationParser {
	//each CSV record must have at least this number of columns to be considered as having a complete Station metadata
	private static final int MINIMUM_COLUMN_COUNT = 16;

	private String _stationName, _provinceName, _cityName, _postalCode, _streetAddr;
	private Float _latitude, _longitude;
	private Integer _stationID, _elevationMeter;
	boolean _isActive;
	private boolean _metReqColumnCount = false;

	public boolean parse(String csvLine) {

/*
104003,VERNON SCIENCE CENTRE,R,1,,P,BRITISH COLUMBIA,2704 HIGHWAY 6,VERNON,CANADA,V1T,V1T 5G5,-8,50.2333,-119.283,476,X,,X,X,,X,,X,,,,,,,,,,,,,,,,,,
60419,CN TOWER,C,0,,N,ONTARIO,CN TOWER,TORONTO,CANADA,M5H,,-5,43.65,-79.38333,108,,,,,,,,,,,,,,,,,,,,,,,,,,
52502,LA TUQUE # 2,R,0,,P,QUEBEC,LA TUQUE # 2,LA TUQUE,CANADA,G9X,,-5,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
105602,OSOYOOS EC WEATHER STN,R,0,,N,BRITISH COLUMBIA,OSOYOOS EC WEAthER STN,OSOYOOS,CANADA,V0H,,-8,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
*/

		// splits the string on a delimiter defined as:
		// zero or more whitespace, a literal comma, zero or more whitespace
		// which will place the words into the list and collapse any whitespace between the words and commas.
		//String[] elems = csvLine.split("\\s*,\\s*"); // IGNORE empty string betwseen delimiters
		String[] elems = csvLine.split("\\s*,\\s*", -1); // KEEP empty string between delimiters

		this._metReqColumnCount = elems.length >= MINIMUM_COLUMN_COUNT;

		if (this._metReqColumnCount) {
			_stationID = safeIntegerConverter(elems[0]); // (elems[0].length() > 0 ? Integer.parseInt(elems[0]) : 0);
			_stationName = elems[1];
			_isActive = (elems[3].equalsIgnoreCase("1"));
			_provinceName = elems[6];
			_streetAddr = elems[7];
			_cityName = elems[8];
			_postalCode = elems[11];
			_latitude = safeFloatConverter(elems[13]);
			_longitude = safeFloatConverter(elems[14]);
			_elevationMeter = safeIntegerConverter(elems[15]);
		}

		return this._metReqColumnCount;
	}

	public boolean isValidRecord() {
		return (this._metReqColumnCount && this._stationID != null && this._stationName != null && this._provinceName != null && this._cityName != null);
	}
	public Integer getStationID() { return this._stationID; }
	public String getStationName() { return this._stationName; }
	public String getProvinceName() { return this._provinceName; }
	public String getCityName() { return this._cityName; }
	public String getStreetAddress() { return this._streetAddr; }
	public String getPostalCode() { return this._postalCode; }
	public Float getLatitude() { return this._latitude; }
	public Float getLongitude() { return this._longitude; }
	public Integer getElevationMeter() { return this._elevationMeter; }


	/**
	 * Convert a String -> Float with careful format checking to avoid NumberFormatException
	 *
	 * @param maybeFloatStr
	 */
	private static Float safeFloatConverter(String maybeFloatStr) {
		Float retval;
		//remove all spaces and +
		String cleanupStr = maybeFloatStr.replaceAll("[ \\+]+","");

		if (cleanupStr.matches("^[\\-]*\\d+\\.*\\d+$"))
			retval = Float.parseFloat(maybeFloatStr);
		else
			retval = null;

		return retval;
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
