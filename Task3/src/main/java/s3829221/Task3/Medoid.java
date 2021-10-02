package s3829221.Task3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import de.jungblut.math.DoubleVector;

public class Medoid implements WritableComparable<Medoid> {
	
	private DoubleVector centre;
	private int kTimesIncremented = 1;
	private int clusterIndex;
	
	public Medoid(DoubleVector centre) {
		super();
		this.centre = centre.deepCopy();
	}
	
	public Medoid(DataPoint centre) {
		super();
		this.centre = centre.getVector().deepCopy();
	}
	
	public Medoid(Medoid centre) {
		super();
		this.centre = centre.centre.deepCopy();
		this.kTimesIncremented = centre.kTimesIncremented;
	}
	
	public Medoid() {
		super();
	}

	public final DoubleVector getMedoidVector() {
		return centre;
	}
	
	public int getClusterIndex() {
		return clusterIndex;
	}
	
	public void setClusterIndex(int clusterIndex) {
		this.clusterIndex = clusterIndex;
	}
	
	public final void plus(DoubleVector centre) {
		this.centre = this.centre.add(centre);
		kTimesIncremented ++;
	}
	
	public final void plus(Medoid centre) {
		this.centre = this.centre.add(centre.getMedoidVector());
		kTimesIncremented += centre.kTimesIncremented;
	}
	
	public final void plus(DataPoint centre) {
		plus(centre.getVector());
	}
	
	public final void dividedByK() {
		this.centre = this.centre.divide(kTimesIncremented);
	}
	
	public final double calculateError(DoubleVector vector) {
		return Math.sqrt(this.centre.subtract(vector).abs().sum());
	}
	
	public final boolean canUpdate(Medoid centre) {
		return calculateError(centre.getMedoidVector()) > 0;
	}

	@Override
	public final void readFields(DataInput in) throws IOException {
		this.centre = DataPoint.readVector(in);
		kTimesIncremented = in.readInt();
		clusterIndex = in.readInt();
	}

	@Override
	public final void write(DataOutput out) throws IOException {
		DataPoint.writeVector(centre, out);
		out.writeInt(kTimesIncremented);
		out.writeInt(clusterIndex);
	}

	@Override
	public final int compareTo(Medoid other) {
		return Integer.compare(clusterIndex, other.clusterIndex);
	}

	@Override
	public final int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((centre == null) ? 0 : centre.hashCode());
		return result;
	}
	
	@Override
	public final boolean equals(Object obj) {
		if (this == obj)
			return true;
		
		if (obj == null)
			return false;
		
		if (getClass() != obj.getClass())
			return false;
		
		Medoid other = (Medoid) obj;
		
		if (centre == null) {
			if (other.centre != null)
				return false;
		} else if (!centre.equals(other.centre))
			return false;
		
		return true;
	}
	
	@Override
	public final String toString() {
		return "Medoid [centre=" + centre + "]";
	}
}
