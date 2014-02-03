
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
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
//import org.jdom2.Document;
//import org.jdom2.Element;
//import org.jdom2.JDOMException;
//import org.jdom2.input.SAXBuilder;
import org.jdom.*;
import org.jdom.input.*;
import org.jdom.JDOMException;

public class PageRankPages {
	public static int N;
	public static class MapPages extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {  
		private final static IntWritable one = new IntWritable(1);

		private Text word = new Text();
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			output.collect(word, one);


		}

	}

	public static class ReducePages extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			int result=0;
			while (values.hasNext()) {
				result+= values.next().get();
			}
			output.collect(key, new Text("N="+result));
			N=result;
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("PageRankPages");//job name
		conf.setInputFormat(XmlInputFormat.class); 
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");

		conf.setOutputKeyClass(Text.class); //map
		conf.setOutputValueClass(IntWritable.class); //map

		conf.setMapperClass(MapPages.class);
		conf.setReducerClass(ReducePages.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
