import org.junit.Before;
import org.junit.Test;
import hadooplab.OzoneAnalysis.NAPSStationParser;

import java.text.DecimalFormat;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * JUnit tests of hadooplab.OzoneAnalysis.NAPSStationParser
 *
 * 2014-04-18 - Tri Nguyen
 */
public class StationParserTest {
	private NAPSStationParser staParser1, staParser2, staParser3, staParser4;

	@Before
	public void setUp() {
		staParser1 = new NAPSStationParser();
		staParser1.parse("060432,FRANK MCKECHNIE COMMERCIAL CENTRE _Mississauga_,C,0,,N,ONTARIO,310 BRISTOL ROAD E.,MISSISSAUGA,CANADA,L4Z,L4Z 3V5,-5,43.61583,-79.6525,165,,,,,,X,,,X,,,,,,,,,,,,,,,,,");

		staParser2 = new NAPSStationParser();
		staParser2.parse("105001,MEADOW PARK,R,1,X,Y,BRITISH COLUMBIA,MEADOW PARK,WHISTLER,CANADA,V0N,,-8,50.1439,-122.9611,638,,,X,X,,X,,X,,,,,,,,,,X,,,,,,,,");

		staParser3 = new NAPSStationParser();
		staParser3.parse("129303,WATERLAB,C,1,,Y,NUNAVUT,,IQALUIT,CANADA,X0A,X0A 0H0,-5,63.751619,-68.522433,35,,,X,X,X,X,,,,,,,,,,,,X,,,,,,,,");

		staParser4 = new NAPSStationParser();
		staParser4.parse("52502,LA TUQUE # 2,R,0,,P,QUEBEC,LA TUQUE # 2,LA TUQUE,CANADA,G9X,,-5,,,,,,,,,,,,,,,,,,,,,,,,,,,,,");
	}

	@Test
	public void testValidRecord() {
		NAPSStationParser parserBogus = new NAPSStationParser();
		parserBogus.parse("50128,A�ROPORT DE MONTR�AL 1,C,1,X,Y,QUEBEC,90-A RUE HERVE-SAINT-MARTIN -  DORVAL,MONTREAL");

		assertTrue("Record1", staParser1.isValidRecord());
		assertTrue("Record2", staParser2.isValidRecord());
		assertTrue("Record3", staParser3.isValidRecord());
		assertTrue("Record4", staParser4.isValidRecord());
		assertFalse("Parser with Corrupted Record", parserBogus.isValidRecord());
	}

	@Test
	public void testStationID() {
		assertEquals("Record1", staParser1.getStationID(), new Integer(60432));
		assertEquals("Record2", staParser2.getStationID(), new Integer(105001));
		assertEquals("Record3", staParser3.getStationID(), new Integer(129303));
		assertEquals("Record4", staParser4.getStationID(), new Integer(52502));
	}

	@Test
	public void testStationName() {
		assertEquals("Record1", staParser1.getStationName(), "FRANK MCKECHNIE COMMERCIAL CENTRE _Mississauga_");
		assertEquals("Record2", staParser2.getStationName(), "MEADOW PARK");
		assertEquals("Record3", staParser3.getStationName(), "WATERLAB");
		assertEquals("Record4", staParser4.getStationName(), "LA TUQUE # 2");
	}

	@Test
	public void testProvinceName() {
		assertEquals("Record1", staParser1.getProvinceName(), "ONTARIO");
		assertEquals("Record2", staParser2.getProvinceName(), "BRITISH COLUMBIA");
		assertEquals("Record3", staParser3.getProvinceName(), "NUNAVUT");
		assertEquals("Record3", staParser4.getProvinceName(), "QUEBEC");
	}

	@Test
	public void testCityName() {
		assertEquals("Record1", staParser1.getCityName(), "MISSISSAUGA");
		assertEquals("Record2", staParser2.getCityName(), "WHISTLER");
		assertEquals("Record3", staParser3.getCityName(), "IQALUIT");
		assertEquals("Record3", staParser4.getCityName(), "LA TUQUE");
	}


	/**
	 * Test LatLong on 4 decimals
	 */
	@Test
	public void testLatitudeElev() {
		DecimalFormat df = new DecimalFormat("#.####");
		assertEquals("Lat  Record1", df.format(staParser1.getLatitude()), "43.6158");
		assertEquals("Long Record1", df.format(staParser1.getLongitude()), "-79.6525");
		assertEquals("Elev Record1", staParser1.getElevationMeter(), new Integer(165));

		assertEquals("Lat  Record2", df.format(staParser2.getLatitude()), "50.1439");
		assertEquals("Long Record2", df.format(staParser2.getLongitude()), "-122.9611");
		assertEquals("Elev Record2", staParser2.getElevationMeter(), new Integer(638));

		assertEquals("Lat  Record3", df.format(staParser3.getLatitude()), "63.7516");
		assertEquals("Long Record3", df.format(staParser3.getLongitude()), "-68.5224");
		assertEquals("Elev Record3", staParser3.getElevationMeter(), new Integer(35));

		assertNull("Lat  Record4", staParser4.getLatitude());
		assertNull("Long Record4", staParser4.getLongitude());
		assertNull("Elev Record4", staParser4.getElevationMeter());
	}
}
