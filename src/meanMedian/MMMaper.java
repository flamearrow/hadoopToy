package meanMedian;

import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MMMaper extends Mapper<Object, Text, Text, MapWritable> {
	Text one = new Text("one");
	MapWritable out = new MapWritable();

	protected void map(Object key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			Text next = new Text(itr.nextToken());
			if (!out.containsKey(next)) {
				out.put(new Text(next), new IntWritable(1));
			} else {
				// should be able to update directly
				IntWritable newV = (IntWritable) out.get(next);
				newV.set(newV.get() + 1);
			}
		}
	}

	// callback for the end of the entire job, we only write one tuple to output
	// which is <one, Histogram>
	protected void cleanup(Context context) throws java.io.IOException,
			InterruptedException {
		context.write(one, out);
	}
}
