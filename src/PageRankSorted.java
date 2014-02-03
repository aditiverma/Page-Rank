
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
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
//import org.jdom2.Document;
//import org.jdom2.Element;
//import org.jdom2.JDOMException;
//import org.jdom2.input.SAXBuilder;
import org.jdom.*;
import org.jdom.input.*;
import org.jdom.JDOMException;
	
	public class PageRankSorted{
		public static int N;
		public static class MapSorted extends MapReduceBase implements Mapper<LongWritable, Text, Double, Text> {  
			private Text word = new Text();
	      public void map(LongWritable key, Text value, OutputCollector<Double, Text> output, Reporter reporter) throws IOException {
	    	  //word.set("a");  
	    	  String []values = value.toString().split("\t");
	    	//  SortedRanks s = new SortedRanks();
	        Double val = Double.valueOf(values[1]);
	        N=215549;//remove the hardcoding
	        if(val>5/N)
	        {
	    	  output.collect(val, new Text(values[0]));
	        }
	    	  //output.collect(new Text(values[1]), new Text(values[0]));
		
	      }
	        
	    }
		
	
	    public static class ReduceSorted extends MapReduceBase implements Reducer<Double, Text, Double, Text> {
	    	public void reduce(Double key, Iterator<Text> values, OutputCollector<Double, Text> output, Reporter reporter) throws IOException {
	    	//	int result=0;
	    	//	while(values.hasNext())
	    	//	{
	    	Text value= values.next();
	         output.collect(key, value);
	    	//	output.collect(new Text(key), value);
	    	//	}
	        //should key val be interchanged?
	      }
	    }	
	    public static void main(String[] args) throws Exception {
	      JobConf conf = new JobConf(PageRankSorted.class);
	      conf.setJobName("PageRankSorted");//job name
	      conf.setInputFormat(TextInputFormat.class); 
	       conf.setOutputKeyClass(Double.class); //map
	      conf.setOutputValueClass(Text.class); //map
	     
	      conf.setOutputKeyComparatorClass(LongWritable.DecreasingComparator.class);
	      //setOutputValueGroupingComparator(Class)
	      
	      conf.setMapperClass(MapSorted.class);
	   //  conf.setCombinerClass(Reduce.class);
	      conf.setReducerClass(ReduceSorted.class);
	    // conf.setNumReduceTasks(1);
	      conf.setOutputFormat(TextOutputFormat.class);
	    //  System.out.println("no. of map tasks"+conf.getNumMapTasks());-different from no. of times map() is called
	      FileInputFormat.setInputPaths(conf, new Path(args[0]));
	      FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	      JobClient.runJob(conf);
	    }
	}
	