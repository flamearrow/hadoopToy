package externalSort;

import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
	protected void reduce(IntWritable key, Iterable<Text> values,
			Context context) throws java.io.IOException, InterruptedException {
		int count = 0;
		Iterator<Text> itr = values.iterator();
		while (itr.hasNext()) {
			count += 1;
			itr.next();
		}
		Text k = new Text(key.toString());
		context.write(k, new IntWritable(count));
	}
}
