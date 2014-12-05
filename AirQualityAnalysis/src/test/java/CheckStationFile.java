import tradoop.OzoneAnalysis.NAPSStationLookup;
import tradoop.OzoneAnalysis.NAPSStationParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Quality Check the NAPS Station file & the HadoopLab.OzoneAnalysis.NAPSStationLookup class
 * Results:
 * Station File: /home/tri/Documents/HadoopLab/AirQualityAnalysis/src/main/resources/Stations_v28012014.csv
 * TOTAL   Record..:    710
 * Valid   Record..:    709
 * Invalid Record..:      1 (have less than 16 columns)
 * Invalid Location:      0
 * Parse Error.....:      0
 * NULL found......:      1

 * 2014-04-19 - Tri Nguyen
 */
public class CheckStationFile {
	private static final String STATION_LOCAL_ABS_FILENAME = "/home/tri/Documents/HadoopLab/AirQualityAnalysis/src/main/resources/Stations_v28012014.csv";
	private static final int MINIMUM_COLUMN_COUNT = 16;

	public static void main(String[] args) throws IOException {
		BufferedReader buffReader = null;
		NAPSStationParser staParser = new NAPSStationParser();
		NAPSStationLookup stationLookup = new NAPSStationLookup();

		stationLookup.initialize(new File(STATION_LOCAL_ABS_FILENAME));

		String currLine = "";
		int lineCount = 0, parseErrorCount = 0, NullFoundCount = 0, validRecordCount = 0;
		int invalidRecordCount = 0, invalidStaLocCount = 0;
		String stationLocation;

		try {
			buffReader = new BufferedReader(new FileReader(STATION_LOCAL_ABS_FILENAME));
			//buffReader = new BufferedReader(new InputStreamReader(new FileInputStream(OZONE_LOCAL_FILENAME)));
			while ((currLine = buffReader.readLine()) != null) {
				lineCount++;

				try {
					staParser.parse(currLine);

					if (staParser.isValidRecord()) {
						validRecordCount++;
						stationLocation = stationLookup.getStationLocation(staParser.getStationID());
						if (stationLocation == null || stationLocation.length() < 1) invalidStaLocCount++;
					}
					else {
						invalidRecordCount++;
						System.out.printf("Line%5d: %s\n", lineCount, currLine);
					}

					if (staParser.getStationID() == null || staParser.getStationName() == null || staParser.getProvinceName() == null || staParser.getCityName() == null) {
						NullFoundCount++;
						System.out.printf("Line%5d: StationID:%s, Name:%s, Prov:%s, City:%s\n", lineCount, staParser.getStationID(), staParser.getStationName(), staParser.getProvinceName(), staParser.getCityName());
					}
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
			System.out.println("-------------------------------------------------------------");
			System.out.printf("Station File: %s\n", STATION_LOCAL_ABS_FILENAME);
			System.out.printf("TOTAL   Record..: %6d\n", lineCount);
			System.out.printf("Valid   Record..: %6d\n", validRecordCount);
			System.out.printf("Invalid Record..: %6d (have less than %d columns)\n", invalidRecordCount, MINIMUM_COLUMN_COUNT);
			System.out.printf("Invalid Location: %6d\n", invalidStaLocCount);
			System.out.printf("Parse Error.....: %6d\n", parseErrorCount);
			System.out.printf("NULL found......: %6d\n", NullFoundCount);
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
}
