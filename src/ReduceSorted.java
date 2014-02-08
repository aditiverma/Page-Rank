import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

	public class ReduceSorted extends MapReduceBase implements Reducer<DoubleWritable, Text, Text,DoubleWritable> {
		public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<Text,DoubleWritable> output, Reporter reporter) throws IOException {
			Text value= values.next();
			output.collect(value, key);
		}
	}