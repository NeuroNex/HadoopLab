package hadooplab.OzoneAnalysis;

import java.io.*;
import java.util.*;
//import org.apache.hadoop.io.IOUtils;

/**
 * Build a Map (StationID, "Province Name / City")
 * To allow a lookup of StationID to get the "Province Name / City"
 *
 * 2014-04-19 - Tri Nguyen
 */
public class NAPSStationLookup {
	private Map<Integer, String> _mapStationLocation = new HashMap<Integer, String>();

	/**
	 * Build a Map (StationID, StationLocation) for all the NAPS Stations
	 * This Map will be used later as lookup table by a Reducer to substitute a StationID by StationLocation
	 *
	 * @param fileName the LOCAL path of the file containing the NAPS Station properties (StationID, StationName, Address, LatLong, etc.)
	 */
	public void initialize(File fileName) throws IOException {
		BufferedReader buffReader = null;
		try {
			buffReader = new BufferedReader(new FileReader(fileName));
			//buffReader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			NAPSStationParser parser = new NAPSStationParser();
			String line;
			while ((line = buffReader.readLine()) != null) {
				if (parser.parse(line)) {
					_mapStationLocation.put(
						parser.getStationID(),
						String.format("%s\t%s", parser.getProvinceName(), parser.getCityName()));
				}
			}
		} finally {
			//IOUtils.closeStream(buffReader);
			if (buffReader != null) buffReader.close();
		}
	}

	public String getStationLocation(Integer stationID) {
		String locationName = _mapStationLocation.get(stationID);
		if (locationName == null || locationName.trim().length() == 0) {
			return stationID.toString(); // no match: fall back to ID
		}
		return locationName;
	}

	public Map<Integer, String> getStationIDToLocationMap() {
		return Collections.unmodifiableMap(_mapStationLocation);
	}
}
