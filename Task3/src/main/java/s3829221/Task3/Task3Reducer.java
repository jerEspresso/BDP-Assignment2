package s3829221.Task3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;

import de.jungblut.math.DoubleVector;

public class Task3Reducer extends Reducer<Medoid, DataPoint, Medoid, DataPoint> {

	public static enum Counter {
		CONVERGED;
	}

	private final List<Medoid> medoids = new ArrayList<>();
	private EuclidianDistance euclidianDistance;
	private int iteration = 0;

	@Override
	protected void reduce(Medoid medoid, Iterable<DataPoint> dataPoints, Context context)
			throws IOException, InterruptedException {
		DoubleVector newCentre = null;
		double lowestDist = Double.MAX_VALUE;
		int numOfDataPoints = 0;
		
		for (DataPoint dataPoint : dataPoints) {
			DoubleVector tempMedoid = dataPoint.getVector().deepCopy();
			double totalDist = 0;
			
			for (DataPoint dp : dataPoints) {
				numOfDataPoints ++;
				totalDist =+ euclidianDistance.measureDistance(tempMedoid, dp.getVector());
			}
			
			double averageDist = totalDist / numOfDataPoints;
			
			// Find the new medoid with the lowest dissimilarity
			if (averageDist < lowestDist) {
				lowestDist = averageDist;
				newCentre = dataPoint.getVector();
			}
		}
		
		Medoid newMedoid = new Medoid(newCentre);
		medoids.add(newMedoid);
		
		// Output the new key-value pairs which will be fed into the next iteration
		for (DataPoint dataPoint : dataPoints)
			context.write(newMedoid, dataPoint);
		
		// Check if all medoids are converged
		if (newMedoid.canUpdate(medoid))
			context.getCounter(Counter.CONVERGED).increment(1);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		
		Configuration conf = context.getConfiguration();
		
		Path outputPath = new Path(conf.get("medoid.path"));
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outputPath, true);
		
		try (SequenceFile.Writer centreWriter = SequenceFile.createWriter(fs, conf, outputPath, Medoid.class, IntWritable.class)) {
			final IntWritable dummy = new IntWritable(iteration);
			System.out.println("Iteration - " + iteration);
			
			for (Medoid medoid : medoids) {
				try {
					centreWriter.append(medoid, dummy);
					System.out.println(medoid.toString());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
