package s3829221.Task2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	protected void reduce(Text keyIn, Iterable<DoubleWritable> valuesIn, Context context) throws IOException, InterruptedException {
		int valueOut = 0;
		
		// Aggregate values of the same key
		for (DoubleWritable value : valuesIn)
			valueOut += value.get();
		
		context.write(keyIn, new DoubleWritable(valueOut));
	}
}
