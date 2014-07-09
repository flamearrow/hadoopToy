package externalSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<Object, Text, IntWritable, Text> {
	Text one = new Text("one");

	@Override
	protected void map(Object key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		Integer val = Integer.parseInt(value.toString());
		context.write(new IntWritable(val), one);
	}
}
