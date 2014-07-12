package meanMedian;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class MMReducer extends Reducer<Text, MapWritable, Text, Text> {
	TreeMap<Integer, Integer> map = new TreeMap<Integer, Integer>();
	int total = 0;

	protected void reduce(Text key, Iterable<MapWritable> values,
			Context context) throws java.io.IOException, InterruptedException {
		// combine maps
		for (MapWritable m : values) {
			for (Entry<Writable, Writable> entry : m.entrySet()) {
				int kk = Integer.parseInt(entry.getKey().toString());
				int vv = Integer.parseInt(entry.getValue().toString());
				total += vv;
				if (map.containsKey(kk)) {
					map.put(kk, map.get(kk) + vv);
				} else {
					map.put(kk, vv);
				}
			}
		}
	}

	// cleanup is only called once, calculate mean and median here
	// note for median we need to judge whether the total is even or odd
	// even inputs would have median=(avg of middle two numbers)
	protected void cleanup(Context context) throws java.io.IOException,
			InterruptedException {
		// calculate the median by histogram
		int half = total / 2;
		boolean even = total % 2 == 0;
		Iterator<Integer> itr = map.keySet().iterator();
		double sum = 0;
		double median = -1;
		int first = 0, second = 0;
		boolean firstFound = false;
		while (itr.hasNext()) {
			int curKey = itr.next();
			sum += map.get(curKey) * curKey;
			// we're still in the process of looking for median
			if (half >= 0) {
				half -= map.get(curKey);
				if (even) {
					// find the first one
					if (half == 0) {
						first = curKey;
						firstFound = true;
					}
					// find the second one
					else if (half < 0) {
						// first and second are different
						if (firstFound == true) {
							second = curKey;
							median = ((double) first + (double) second) / 2;
						}
						// first and second are the same
						else {
							median = curKey;
						}
					}
				} else {
					if (half < 0) {
						median = curKey;
					}
				}
			}
		}
		context.write(new Text("median"), new Text("" + median));
		context.write(new Text("mean"), new Text("" + (sum / total)));
	}
}
