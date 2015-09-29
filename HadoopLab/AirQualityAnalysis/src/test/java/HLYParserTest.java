import java.text.DecimalFormat;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import hadooplab.OzoneAnalysis.HLYRecordParser;

import static org.junit.Assert.*;

/**
 * JUnit tests of hadooplab.OzoneAnalysis.HLYRecordParser
 *
 * 2014-04-18 - Tri Nguyen
 */
public class HLYParserTest {
	private HLYRecordParser hlyParser1, hlyParser2, hlyParser3, hlyParser4;

	@Before
	public void setUp() {
		//Case1: ZERO invalid reading
		hlyParser1 = new HLYRecordParser();
		hlyParser1.parse("00610500120120413  40  31  45  33  31  33  35  35  35  35  35  36  38  40  41  42  44  45  45  45  43  45  44  43  42  42  40");

		//Case2: ALL readings are invalid
		hlyParser2 = new HLYRecordParser();
		hlyParser2.parse("00606070920120912-999-999-999-999-999-999-999-999-999-999-999-999-999-999-999-999-999-999-999-999-999-999-999-999-999-999-999");

		//Case3: mix valid/invalid readings, Average is valid
		hlyParser3 = new HLYRecordParser();
		hlyParser3.parse("00612920220120207  32  28  37  33  32  31  31  31  32  33  36  37  36  35  34-999-999-999-999-999-999  28  29  30  31  30  29");

		//Case4: mix valid/invalid readings, Average is invalid
		hlyParser4 = new HLYRecordParser();
		hlyParser4.parse("00606041920121024-999  23  35  34  35  29  23  25  25  26-999-999-999-999  28-999-999-999  31-999  32  33-999  34-999  35  35");
	}

	@Test
	public void testValidRecord() {
		HLYRecordParser parser1 = new HLYRecordParser();
		parser1.parse("Corrupted Record");

		assertTrue("Record1", hlyParser1.isValidRecord());
		assertFalse("Record2", hlyParser2.isValidRecord()); // False: b/c CalcAverage is NULL
		assertTrue("Record3", hlyParser3.isValidRecord());
		assertTrue("Record4", hlyParser4.isValidRecord());
		assertFalse("Parser with Corrupted Record", parser1.isValidRecord());
	}

	@Test
	public void testStationID() {
		assertEquals("Record1", hlyParser1.getStationID(), new Integer(105001));
		assertEquals("Record2", hlyParser2.getStationID(), new Integer(60709));
		assertEquals("Record3", hlyParser3.getStationID(), new Integer(129202));
		assertEquals("Record4", hlyParser4.getStationID(), new Integer(60419));
	}

	@Test
	public void testDate() {
		assertEquals("Record1", hlyParser1.getDateString(), "20120413");
		assertEquals("Record2", hlyParser2.getDateString(), "20120912");
		assertEquals("Record3", hlyParser3.getDateString(), "20120207");
		assertEquals("Record4", hlyParser4.getDateString(), "20121024");
	}

	/**
	 * When DayAverage value = -999 hadooplab.OzoneAnalysis.HLYRecordParser must return NULL instead
	 */
	@Test
	public void testDayAverage() {
		assertEquals("Record1", hlyParser1.getDayAverage(), new Integer(40));
		assertNull("Record2", hlyParser2.getDayAverage());
		assertEquals("Record3", hlyParser3.getDayAverage(), new Integer(32));
		assertNull("Record4", hlyParser4.getDayAverage());
	}

	/**
	 * Calculated value of DayAverage always return a valid float value
	 * EXCEPT when all the 24 readings are invalid, in such a case NULL is returned
	 * As result is float, compare on 2 decimals is enough
	 */
	@Test
	public void testDayAverageCALC() {
		DecimalFormat df = new DecimalFormat("#.##");
		assertEquals("Record1", df.format(hlyParser1.getCalcDayAverage()), "39.46");
		assertNull("Record2", hlyParser2.getCalcDayAverage());
		assertEquals("Record3", df.format(hlyParser3.getCalcDayAverage()), "32.11");
		assertEquals("Record4", df.format(hlyParser4.getCalcDayAverage()), "30.36");
	}

	/**
	 * The difference between Calculated DayAverage and Precalculated value must be < 0.6F
	 */
	@Test
	public void testCompareDayAverage() {
		final float ACCEPTABLE_DIFF = 0.6F;

		//float fAvg2 = (hlyParser2.getDayAverage() == null ? 0F : new Float(hlyParser2.getDayAverage().toString()));
		//float fAvg4 = (hlyParser4.getDayAverage() == null ? 0F : new Float(hlyParser4.getDayAverage().toString()));

		assertThat("Record1", Math.abs(hlyParser1.getDayAverage() - hlyParser1.getCalcDayAverage()), lessThan(ACCEPTABLE_DIFF));
		//assertThat("Record2", Math.abs(fAvg2 - hlyParser2.getCalcDayAverage()), lessThan(ACCEPTABLE_DIFF));
		assertThat("Record3", Math.abs(hlyParser3.getDayAverage() - hlyParser3.getCalcDayAverage()), lessThan(ACCEPTABLE_DIFF));
		//assertThat("Record4", Math.abs(fAvg4 - hlyParser4.getCalcDayAverage()), lessThan(ACCEPTABLE_DIFF));
	}

	/**
	 * The difference between Calculated DayMin and Precalculated value must be < 1
	 */
	@Test
	public void testCompareDayMin() {
		final int ACCEPTABLE_DIFF = 1;

		assertThat("Record1", Math.abs(hlyParser1.getDayMin() - hlyParser1.getCalcDayMin()), lessThan(ACCEPTABLE_DIFF));
		//assertThat("Record2", Math.abs(hlyParser2.getDayMin() - hlyParser2.getCalcDayMin()), lessThan(ACCEPTABLE_DIFF));
		assertThat("Record3", Math.abs(hlyParser3.getDayMin() - hlyParser3.getCalcDayMin()), lessThan(ACCEPTABLE_DIFF));
		assertThat("Record4", Math.abs(hlyParser4.getDayMin() - hlyParser4.getCalcDayMin()), lessThan(ACCEPTABLE_DIFF));
	}


	/**
	 * The difference between Calculated DayMax and Precalculated value must be < 1
	 */
	@Test
	public void testCompareDayMax() {
		final int ACCEPTABLE_DIFF = 1;

		assertThat("Record1", Math.abs(hlyParser1.getDayMax() - hlyParser1.getCalcDayMax()), lessThan(ACCEPTABLE_DIFF));
		//assertThat("Record2", Math.abs(hlyParser2.getDayMax() - hlyParser2.getCalcDayMax()), lessThan(ACCEPTABLE_DIFF));
		assertThat("Record3", Math.abs(hlyParser3.getDayMax() - hlyParser3.getCalcDayMax()), lessThan(ACCEPTABLE_DIFF));
		assertThat("Record4", Math.abs(hlyParser4.getDayMax() - hlyParser4.getCalcDayMax()), lessThan(ACCEPTABLE_DIFF));
	}


	/**
	 * Custom Matcher to test lessThan()
	 * Example:
	 * - assertThat("Test Float", 3.14F, lessThan(4.2F));
	 * - assertThat("Test Looooong", 123L, lessThan(456L));
	 *
	 * This matcher is an anonymous subclass of the class BaseMatcher.
	 * The JUnit documentation states that you should always extend BasreMatcher rather than implement the Matcher interface yourself. Thus, if new methods are added to Matcher in the future, BaseMatcher can implement them.
	 * Your subclasses will then automatically get those methods too.
	 * Thank you: http://tutorials.jenkov.com/java-unit-testing/matchers.html
	 *
	 * @param expectedLessThanValue: assertThat("Blabla...", valueToTest, lessThan(expectedLessThan));
	 * @return
	 */
	public static Matcher lessThan(final Object expectedLessThanValue) {

		return new BaseMatcher() {

			protected Object _expectValue = expectedLessThanValue;

			public boolean matches(Object valToTest) {
				if (valToTest instanceof Float) {
					return (Float.parseFloat(valToTest.toString()) < Float.parseFloat(_expectValue.toString()));
				}
				else if (valToTest instanceof Double) {
					return (Double.parseDouble(valToTest.toString()) < Double.parseDouble(_expectValue.toString()));
				}
				else if (valToTest instanceof Long) {
					return (Long.parseLong(valToTest.toString()) < Long.parseLong(_expectValue.toString()));
				}
				else { // if (valueGet instanceof Integer) {
					return (Integer.parseInt(valToTest.toString()) < Integer.parseInt(_expectValue.toString()));
				}
			}

			public void describeTo(Description description) {
				description.appendText(_expectValue.toString());
			}
		};
	}


//	/**
//	 * Genereic lessThan
//	 * http://hamcrest.org/JavaHamcrest/javadoc/1.3/org/hamcrest/number/OrderingComparison.html#lessThan%28T%29
//	 *
//	 * @param expected
//	 * @param <T>
//	 * @return
//	 */
//	public static <T extends java.lang.Comparable<T>> Matcher<T> lessThan2(T expected) {
//		return new Matcher() {
//			protected T theExpected = expected;
//
//			public boolean matches(Object valueGet) {
//				return (valueGet.compareTo(expected) == -1);
//			}
//		}
//	}
}
