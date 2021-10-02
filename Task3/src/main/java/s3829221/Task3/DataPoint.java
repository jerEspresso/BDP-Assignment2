package s3829221.Task3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import de.jungblut.math.DoubleVector;
import de.jungblut.math.dense.DenseDoubleVector;

public class DataPoint implements WritableComparable<DataPoint> {

	private DoubleVector vector;
	
	public DataPoint (DataPoint dataPoint) {
		this.vector = dataPoint.getVector();
	}
	
	public DataPoint (DenseDoubleVector vector) {
		this.vector = vector;
	}
	
	public DataPoint (double x, double y) {
		this.vector = new DenseDoubleVector(new double[] {x, y});
	}
	
	public DataPoint() {
		super();
	}

	public DoubleVector getVector() {
		return vector;
	}
	
	@Override
	public final void readFields(DataInput in) throws IOException {
		this.vector = readVector(in);
	}
	
	public static DoubleVector readVector(DataInput in) throws IOException {
		final int length = in.readInt();
		DoubleVector vector = new DenseDoubleVector(length);
		
		for (int i = 0;i < length; i++)
			vector.set(i, in.readDouble());
		
		return vector;
	}

	@Override
	public final void write(DataOutput out) throws IOException {
		writeVector(this.vector, out);
	}

	public static void writeVector(DoubleVector vector, DataOutput out) throws IOException {
		out.writeInt(vector.getLength());
		for (int i = 0; i < vector.getDimension(); i++)
			out.writeDouble(vector.get(i));
	}

	@Override
	public final int compareTo(DataPoint other) {
		return compareVector(this.getVector(), other.getVector());
	}

	public static int compareVector(DoubleVector v1, DoubleVector v2) {
		DoubleVector subtract = v1.subtract(v2);
		return (int) subtract.sum();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((vector == null) ? 0 : vector.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		
		if (obj == null)
			return false;
		
		if (getClass() != obj.getClass())
			return false;
		
		DataPoint other = (DataPoint) obj;
		
		if (vector == null) {
			return other.vector == null;
		} else 
			return vector.equals(other.vector);
	}
	
	@Override
	public String toString() {
		return vector.toString();
	}
}
