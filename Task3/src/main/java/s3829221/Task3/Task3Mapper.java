package s3829221.Task3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;

public class Task3Mapper extends Mapper<Medoid, DataPoint, Medoid, DataPoint> {

	private final List<Medoid> medoids = new ArrayList<>();
	private EuclidianDistance euclidianDistance;

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		Configuration conf = context.getConfiguration();
		Path medoidPath = new Path(conf.get("medoid.path"));
		FileSystem fs = FileSystem.get(conf);
		
		try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, medoidPath, conf)) {
			Medoid key = new Medoid();
			IntWritable value = new IntWritable();
			int index = 0;
			while (reader.next(key, value)) {
				Medoid medoid = new Medoid(key);
				medoid.setClusterIndex(index++);
				medoids.add(medoid);
			}
		}
		
		euclidianDistance = new EuclidianDistance();
	}
	
	@Override
	protected void map(Medoid key, DataPoint dataPoint, Context context)
			throws IOException, InterruptedException {
		Medoid nearestMedoid = null;
		double nearestDist = Double.MAX_VALUE;
		
		for (Medoid medoid : medoids) {
			double dist = euclidianDistance.measureDistance(medoid.getMedoidVector(), dataPoint.getVector());
			if (dist < nearestDist || nearestMedoid == null) {
				nearestDist = dist;
				nearestMedoid = medoid;
			}
		}
		
		context.write(nearestMedoid, dataPoint);
	}
}
