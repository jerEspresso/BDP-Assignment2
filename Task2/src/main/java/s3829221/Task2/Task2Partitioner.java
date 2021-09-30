package s3829221.Task2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Task2Partitioner extends Partitioner<Text, DoubleWritable> {

	@Override
	public int getPartition(Text key, DoubleWritable value, int numPartitions) {
		// Extract the left word
		String leftStr = key.toString().split(" ")[0];
		Text leftWord = new Text(leftStr);
		
		// Assign reducer based on the left word
		return leftWord.hashCode() % numPartitions;
	}
}
