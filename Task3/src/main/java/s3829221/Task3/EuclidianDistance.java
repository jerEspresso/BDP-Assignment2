package s3829221.Task3;

import org.apache.commons.math3.util.FastMath;

import de.jungblut.math.DoubleVector;

public class EuclidianDistance {
	
	private static final EuclidianDistance DISTANCE = new EuclidianDistance();

	public double measureDistance(double[] set1, double[] set2) {
		double sum = 0;
		int length = set1.length;
		
		for (int i = 0; i < length; i++) {
			double diff = set2[i] - set1[i];
			sum += (diff * diff);
		}

		return FastMath.sqrt(sum);
	}

	public double measureDistance(DoubleVector v1, DoubleVector v2) {
		if (v1.isSparse() || v2.isSparse())
			return FastMath.sqrt(v2.subtract(v1).pow(2).sum());
		else 
			return measureDistance(v1.toArray(), v2.toArray());
	}

	public static EuclidianDistance get() {
		return DISTANCE;
	}
}
