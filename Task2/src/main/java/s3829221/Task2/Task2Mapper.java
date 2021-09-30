package s3829221.Task2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task2Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	final private static DoubleWritable ONE = new DoubleWritable(1);
	final private static int DISTANCE = 4;
	StringTokenizer tokenizer;
	
	@Override
	protected void map(LongWritable offset, Text valueIn, Context context) throws IOException, InterruptedException {
		// Split document into tokens using the default delimiters
		tokenizer = new StringTokenizer(valueIn.toString());
		
		int numOfTokens = tokenizer.countTokens();
		
		// Skip if the current input split only contains one token
		if (numOfTokens <= 1)
			return;
		
		// Put tokens into array
		String[] tokens = new String[numOfTokens];
		for (int i = 0; i < numOfTokens; i++) {
			tokens[i] = tokenizer.nextToken();
		}
		
		// Pair each token with each of its neighbours
		for (int i = 0; i < numOfTokens; i++) {
			// Find the index of the start neighbour and the end neighbour
			int start = (i - DISTANCE < 0) ? 0 : (i - DISTANCE);
			int end = (i + DISTANCE >= numOfTokens) ? (numOfTokens - 1) : (i + DISTANCE);
			
			// Initiate the number of word pairs with the same left word
			int total = 0;
			
			for (int j = start; j <= end; j++) {
				// Skip the current token itself
				if (i == j)
					continue;
				// Skip identical neighbours
				if (tokens[i].equals(tokens[j]))
					continue;
				// Emit an intermediate key-value pair for each word pair
				Text keyOut = new Text(tokens[i] + " " + tokens[j]);
				context.write(keyOut, ONE);
				total += 1;
			}
			
			// Emit the key-value pair for the left word
			context.write(new Text(tokens[i] + " *"), new DoubleWritable(total));
		}
	}
}
