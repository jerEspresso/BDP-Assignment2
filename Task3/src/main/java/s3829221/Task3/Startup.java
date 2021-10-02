package s3829221.Task3;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Startup {
	
	private static final Log LOG = LogFactory.getLog(Startup.class);

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		int iteration = 1;
		
		Configuration conf = new Configuration();
		
		String outputPathString = args[1];
		
		Path intputPath = new Path(args[0]);
		Path outputPath = new Path(outputPathString + "/depth_1");
		Path dataPointPath = new Path("intermediate/datapoints.seq");
		Path medoidPath = new Path("intermediate/medoids.seq");
		
		conf.set("iteration", iteration + "");
		conf.set("medoid.path", medoidPath.toString());
		
		Job job = Job.getInstance(conf, "Task 3 - Iteration " + iteration);
		
		job.setJarByClass(Startup.class);
		job.setMapperClass(Task3Mapper.class);
		job.setReducerClass(Task3Reducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Medoid.class);
		job.setOutputValueClass(DataPoint.class);
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, dataPointPath);
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath))
			fs.delete(outputPath, true);
		if (fs.exists(dataPointPath))
			fs.delete(dataPointPath, true);
		if (fs.exists(medoidPath))
			fs.delete(medoidPath, true);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		
		int numOfClusters = Integer.parseInt(args[2]);
		
		generateDataPoints(intputPath, dataPointPath, conf, fs);
		generateMedoid(intputPath, medoidPath, conf, fs, numOfClusters);
		
		job.waitForCompletion(true);
		
		// Move on to the next iteration if not converged
		long counter = job.getCounters().findCounter(Task3Reducer.Counter.CONVERGED).getValue();
		iteration ++;
		while (counter > 0) {
			conf = new Configuration();
			
			conf.set("iteration", iteration + "");
			conf.set("medoid.path", medoidPath.toString());
			
			job = Job.getInstance(conf, "Task 3 - Iteration " + iteration);
			
			job.setJarByClass(Startup.class);
			job.setMapperClass(Task3Mapper.class);
			job.setReducerClass(Task3Reducer.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setOutputKeyClass(Medoid.class);
			job.setOutputValueClass(DataPoint.class);
			job.setNumReduceTasks(1);
			
			dataPointPath = new Path(outputPathString + "/depth_" + (iteration - 1) + "/");
			outputPath = new Path(outputPathString + "/depth_" + iteration + "/");
			
			FileInputFormat.addInputPath(job, dataPointPath);
			if (fs.exists(outputPath))
				fs.delete(outputPath, true);
			
			FileOutputFormat.setOutputPath(job, outputPath);
			
			job.waitForCompletion(true);
			
			iteration ++;
			
			counter = job.getCounters().findCounter(Task3Reducer.Counter.CONVERGED).getValue();
		}
		
		Path resultPath = new Path(outputPathString + "/depth_" + (iteration - 1) + "/");
		
		FileStatus[] statuses = fs.listStatus(resultPath);
		for (FileStatus status : statuses) {
			if (!status.isDir()) {
				Path path = status.getPath();
				if (!path.getName().equals("_SUCCESS")) {
					LOG.info("FOUND" + path.toString());
					try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf)) {
						Medoid key = new Medoid();
						DataPoint value = new DataPoint();
						while (reader.next(key, value))
							LOG.info(key + " / " + value);
					}
				}
			}
		}
	}
	
	public static void generateDataPoints(Path inputPath, Path outputPath, Configuration conf, FileSystem fs) throws IOException {
		Scanner inputStream = new Scanner(new File(inputPath.toString()));
		
		try (SequenceFile.Writer dataWriter = SequenceFile.createWriter(fs, conf, outputPath, Medoid.class, DataPoint.class)) {
			// Create a data point for each line
			while (inputStream.hasNextLine()) {
				String[] vectorValue = inputStream.nextLine().split(" ");
				double x = Double.parseDouble(vectorValue[0]);
				double y = Double.parseDouble(vectorValue[1]);
				
				dataWriter.append(new Medoid(new DataPoint(0, 0)), new DataPoint(x, y));
			}
		}
		
		inputStream.close();
	}
	
	public static void generateMedoid(Path inputPath, Path outputPath, Configuration conf, FileSystem fs, int numOfClusters) throws IOException {
		Scanner inputStream = new Scanner(new File(inputPath.toString()));
		
		try (SequenceFile.Writer centreWriter = SequenceFile.createWriter(fs, conf, outputPath, Medoid.class, IntWritable.class)) {
			final IntWritable dummy = new IntWritable(0);
			
			// Create an initial medoid for every 100 lines
			for (int i = 0; i < numOfClusters; i++) {
				// Skip 99 lines
				for (int j = 0; j < 99; j ++) 
					inputStream.nextLine();
				
				String[] medoidValue = inputStream.nextLine().split(" ");
				double x = Double.parseDouble(medoidValue[0]);
				double y = Double.parseDouble(medoidValue[1]);
				
				centreWriter.append(new Medoid(new DataPoint(x, y)), dummy);
			}
		}
		
		inputStream.close();
	}
}
