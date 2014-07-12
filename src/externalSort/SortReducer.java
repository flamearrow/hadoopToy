package externalSort;

import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
	protected void reduce(IntWritable key, Iterable<Text> values,
			Context context) throws java.io.IOException, InterruptedException {
		Iterator<Text> itr = values.iterator();
		while (itr.hasNext()) {
			itr.next();
		}
		Text k = new Text(key.toString());
		// we can write nothing to value - key is sorted
		// context.write(k, new IntWritable(count));
		context.write(k, null);
	}
}
