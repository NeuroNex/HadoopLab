import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable Composite Key containing (ProvinceName, CityName, Average Ozone Value)
 * to be used for Secondary Sort in a Reducer to sort the results by Value (instead of by key)
 *
 * 2014-04-20 - Tri.Nguyen
 */
public class LocationMeasureCompoKey implements WritableComparable<LocationMeasureCompoKey> {

	private String _K1location; // = new Text();
	private int _K2averageOzone;

	public LocationMeasureCompoKey() {
	}

	public LocationMeasureCompoKey(String locationName, int avgOzoneValue) {
		set(locationName, avgOzoneValue);
	}

	public void set(String locationName, int avgOzoneValue) {
		//this._K1location = new Text(locationName);
		//this._K1location.set(locationName);
		this._K1location = locationName;
		this._K2averageOzone = avgOzoneValue;
	}

	public String getLocation() {
		return _K1location;
	}

	public int getAverageOzone() {
		return _K2averageOzone;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//_K1location.write(out);
		//out.writeUTF(_K1location.toString());
		out.writeUTF(_K1location);
		out.writeInt(_K2averageOzone);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		//_K1location.readFields(in);
		//_K1location.set(in.readUTF());
		_K1location = in.readUTF();
		_K2averageOzone = in.readInt();
	}

	@Override
	public int hashCode() {
		return String.format("%s%d", this._K1location, this._K2averageOzone).hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof LocationMeasureCompoKey) {
			LocationMeasureCompoKey lmck = (LocationMeasureCompoKey) o;
			return _K1location.equals(lmck.getLocation()) && _K2averageOzone == lmck.getAverageOzone();
		}
		return false;
	}

	@Override
	public String toString() {
		return String.format("%s\t%d", _K1location, _K2averageOzone);
	}

	/**
	 * Compare between instances of this class
	 * Pay attention to use the comparator specific to the type of each member of the composite key
	 */
	@Override
	public int compareTo(LocationMeasureCompoKey lmck) {
		// Priority1: Compare on Primary Member (the principal key of the composite key
		int cmp = this._K1location.compareTo(lmck._K1location);
		if (cmp != 0) {
			// Priority2: in case the Primary Member is identical, compare on 2nd key
			//cmp = (this._K2averageOzone < lmck._K2averageOzone ? -1 : (this._K2averageOzone == lmck._K2averageOzone ? 0 : 1));
			cmp = this.compareInt(this._K2averageOzone, lmck._K2averageOzone);
		}
		return cmp;
	}


	/**
	 * Util method for comparing two int
	 */
	public static int compareInt(int a, int b) {
		return (a < b ? -1 : (a == b ? 0 : 1));
	}

	public static int compareIntDescending(int a, int b) {
		return (a < b ? 1 : (a == b ? 0 : -1));
	}
}
