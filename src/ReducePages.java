import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

	public class ReducePages extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			int result=0;
			while (values.hasNext()) {
				result+= values.next().get();
			}
			//PageRank.N=2;//not assigning
			output.collect(key, new Text("N="+result));
			//N=result;
			
		}
	}