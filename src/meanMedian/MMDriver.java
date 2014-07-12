package meanMedian;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// The idea is to let each Mapper iterate through the file and output a single MapWritable with <Num, Cnt> to Reducer
// then Reducer combine all MapWritable into a single Map so that we get a histogram of the total <Num, Cnt>
// then it's trivial to compute mean and median
// Map output: <Text, MapWritable>
// Reducer output: <Text, Text>

//
// an improvement would be getting rid of the MapWritable and use two map reduce process
//   first round: word count, sort the key, we end up having Num, Cnt as output
// Mapper output: <Text, IntWritable>
// Reducer output: <Text, Text>
//   second round: grabbing all the Num, Cnt to Reducer and do the same thing
// Mapper output: <IntWritable, IntWritable>
// Reducer output: <Text, Text>
// if we can pass the total cnt to the second round, we can get rid of combing numbers in the second reducer
// because all incoming entries would be sorted by key
public class MMDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: mm <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "median");
		job.setJarByClass(MMDriver.class);
		job.setMapperClass(MMMaper.class);
		job.setReducerClass(MMReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
