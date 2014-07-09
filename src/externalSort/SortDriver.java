package externalSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SortDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: SortDriver <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "external sort");
		job.setJarByClass(SortDriver.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		// combiner is a mini reducer that combines value and counts during
		// mapping, it helps to reduce the number of key value pairs sent to
		// reducer
		// it implements the same interface as reducer,
		// note the IN/OUT KEY/VALUE type should be applied correctly
		// in conjunction with Maper/Reducer

		// job.setCombinerClass(SortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
