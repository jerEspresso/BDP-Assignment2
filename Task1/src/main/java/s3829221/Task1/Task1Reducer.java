package s3829221.Task1;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text keyIn, Iterable<IntWritable> valuesIn, Context context) throws IOException, InterruptedException {
		int valueOut = 0;
		
		// Aggregate values of the same key
		for (IntWritable value : valuesIn)
			valueOut += value.get();
		
		context.write(keyIn, new IntWritable(valueOut));
	}
}
