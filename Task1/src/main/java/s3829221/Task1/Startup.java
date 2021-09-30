package s3829221.Task1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Startup 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException {
    	Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Task 1");
		
		job.setJarByClass(Startup.class);
		job.setMapperClass(Task1Mapper.class);
		job.setReducerClass(Task1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
