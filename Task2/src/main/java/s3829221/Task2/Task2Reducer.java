package s3829221.Task2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	private DoubleWritable total = new DoubleWritable();
	private DoubleWritable relativeFreq = new DoubleWritable();
	private String currentWord = null;

	@Override
	protected void reduce(Text keyIn, Iterable<DoubleWritable> valuesIn, Context context)
			throws IOException, InterruptedException {
		String[] wordPair = keyIn.toString().split(" ");
		String leftWord = wordPair[0];
		String rightWord = wordPair[1];

		// Aggregate all (A, *) keys
		if (rightWord.equals("*")) {
			if (leftWord.equals(currentWord))
				total.set(total.get() + getTotal(valuesIn));
			// Clear the total when the left word changes
			else {
				currentWord = leftWord;
				total.set(0);
				total.set(getTotal(valuesIn));
			}
		// Calculate the relative frequency for each word pair
		} else {
			relativeFreq.set(getTotal(valuesIn) / total.get());
			context.write(keyIn, relativeFreq);
		}
	}

	private double getTotal(Iterable<DoubleWritable> valuesIn) {
		double count = 0;
		for (DoubleWritable value : valuesIn)
			count += value.get();

		return count;
	}
}
