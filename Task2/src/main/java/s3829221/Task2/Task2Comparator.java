package s3829221.Task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Task2Comparator extends WritableComparator {

	protected Task2Comparator() {
		super(Text.class, true);
	}

	@Override
	public int compare(WritableComparable k1, WritableComparable k2) {
		Text wordPair1 = (Text) k1;
		Text wordPair2 = (Text) k2;
		
		int compareResult = wordPair1.compareTo(wordPair2);
		
		if (compareResult != 0)
			return compareResult;
		
		if (wordPair1.toString().split(" ")[1].equals("*"))
			return -1;
		else if (wordPair2.toString().split(" ")[1].equals("*"))
			return 1;
		
		return wordPair1.toString().split(" ")[1].compareTo(wordPair2.toString().split(" ")[1]);
	}
}
