
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.jdom.*;
import org.jdom.input.*;
import org.jdom.JDOMException;

public class PageRankSorted{
	public static int N;
	public static class MapSorted extends MapReduceBase implements Mapper<LongWritable, Text, Double, Text> {  
		private Text word = new Text();
		public void map(LongWritable key, Text value, OutputCollector<Double, Text> output, Reporter reporter) throws IOException {
			String []values = value.toString().split("\t");
			Double val = Double.valueOf(values[1]);
			N=215549;//remove the hardcoding
			if(val>5/N)
			{
				output.collect(val, new Text(values[0]));
			}
		}

	}


	public static class ReduceSorted extends MapReduceBase implements Reducer<Double, Text, Double, Text> {
		public void reduce(Double key, Iterator<Text> values, OutputCollector<Double, Text> output, Reporter reporter) throws IOException {
			Text value= values.next();
			output.collect(key, value);
		}
	}	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(PageRankSorted.class);
		conf.setJobName("PageRankSorted");//job name
		conf.setInputFormat(TextInputFormat.class); 
		conf.setOutputKeyClass(Double.class); //map
		conf.setOutputValueClass(Text.class); //map
		conf.setOutputKeyComparatorClass(LongWritable.DecreasingComparator.class);
		conf.setMapperClass(MapSorted.class);
		conf.setReducerClass(ReduceSorted.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
