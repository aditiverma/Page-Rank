
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.jdom.*;
import org.jdom.input.*;
import org.jdom.JDOMException;
	
	public class PageRankIterative {
	public static int N;
		public static class MapIterative extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {  
			//private final static IntWritable one = new IntWritable(1);
	      
			private Text word = new Text();
	      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    	   String line = value.toString();
	    	  //StringTokenizer tokenizer = new StringTokenizer(line);
	    	   int indexBeforeRank = line.indexOf("\t");
	    	   if(indexBeforeRank!=-1)//
	    	   {
	    	   String inlinkPage = line.substring(0, indexBeforeRank);
	    	   String rest = line.substring(indexBeforeRank+1, line.length());
	    	   String []splitOutLinks = rest.split("\t");
	    	   //check if first is the rank (integer), its not an integer for the first iteration
	    	   String rank="1";
	    	   int start=0;
	    	   if(isInteger(splitOutLinks[0]))
	    	   {
	    		   rank = splitOutLinks[0];
	    		   start=1;
	    	   }
	    	   String outValue = rank+"\t"+inlinkPage+"\t"+(splitOutLinks.length-1);
	    	   StringBuilder allOutLinks = new StringBuilder();
	    	   for(int i=start;i<splitOutLinks.length;i++)
	    	   {
	    		   String outkey= splitOutLinks[i];
	    		   output.collect(new Text(outkey), new Text(outValue));
	    		   allOutLinks.append(splitOutLinks[i]+"\t");
	    	   }
	    	  output.collect(new Text(inlinkPage), new Text("\t\t"+allOutLinks.toString()));
	    
	    	   }//if tab present
	    	   }
	      public static boolean isInteger(String s) {
	    	    try { 
	    	        Integer.parseInt(s); 
	    	    } catch(NumberFormatException e) { 
	    	        return false; 
	    	    }
	    	    // only got here if we didn't return false
	    	    return true;
	    	}  
	    }
	
	    public static class ReduceIterative extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	    	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    		double sumRank=0;
	    		String outLinks="";
	    		Text outValue = new Text();
	    		boolean isValidLink=false;
	    		while (values.hasNext()) {
	        	String tmp = values.next().toString();
	        	if(tmp.indexOf("\t\t")==0)
	        	{
	        		//list of out links for an in link
	        		outLinks=tmp.substring(1, tmp.length());
	        		isValidLink = true;
	        	}
	        	else
	        	{
	        		//inlink info for an outlink
	        		String splitValues[] = tmp.split("\t");
	        		if(Double.valueOf(splitValues[2])>0)
	        				{
	        				sumRank += Double.valueOf(splitValues[0])/Double.valueOf(splitValues[2]);
	        				}
	        	}
	        }
	        
	        
//	        String part2Output="/user/hduser/files-output2/part-00000"; //change this location
//	    	StringBuilder sb = new StringBuilder();
//	     	 try{
//                 Path pt=new Path(part2Output);
//                 FileSystem fs = FileSystem.get(new Configuration());
//                 BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
//                 String line;
//                 line=br.readLine();
//                 while (line != null){
//                         line=br.readLine();
//                 }
//                String number = sb.substring(sb.indexOf("=")+1, sb.length());
//                N= Integer.parseInt(number);
//	     	 }catch(Exception e){
//	     	 }
	    	    
	        
	      N=215549;//remove the hardcoding
	        sumRank = sumRank*.85 +.15/N;
	        outValue.set(String.valueOf(sumRank) +"\t"+ outLinks);
	        if(isValidLink)
	        {
            output.collect(key, outValue);
	        }
	      }
	    }
	
	    public static void main(String[] args) throws Exception {
	    	String inputDir =args[0], outputDir=args[1];    	
//	    	String part2Output="/user/hduser/files-output2/part-00000"; //change this location
//	    	StringBuilder sb = new StringBuilder();
//	     	 try{
//                 Path pt=new Path(part2Output);
//                 FileSystem fs = FileSystem.get(new Configuration());
//                 BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
//                 String line;
//                 line=br.readLine();
//                 while (line != null){
//                         line=br.readLine();
//                 }
//                String number = sb.substring(sb.indexOf("=")+1, sb.length());
//                N= Integer.parseInt(number);
//	     	 }catch(Exception e){
//	     	 }
//	    	    
	    	    
	    	
	      for(int i=0;i<3;i++) //input1 is getting modified and deleted
	      {
	    	  JobConf conf = new JobConf(PageRankIterative.class);
		      conf.setJobName("PageRankIterative"+i);//job name
		      conf.setMapperClass(MapIterative.class);
		      conf.setReducerClass(ReduceIterative.class);
		      
		      conf.setInputFormat(TextInputFormat.class);       
		      conf.setOutputKeyClass(Text.class);
		      conf.setOutputValueClass(Text.class);
		      conf.setOutputFormat(TextOutputFormat.class);
		      FileInputFormat.setInputPaths(conf, new Path(inputDir));//initial input is the output of Part 1, with pagerank of 1 included per line, (also included N as static)
		      FileOutputFormat.setOutputPath(conf, new Path(outputDir));
		     
		      JobClient.runJob(conf);
		      String tmp = inputDir;
		      inputDir=outputDir;
		      outputDir=tmp;
		      if(i==1)//1
		      {
		    	outputDir = "outiternext";  
		      }else if(i==0)
		      {
		    	  outputDir = "outputiter1";
		      }else
		      {
		      deleteDir(outputDir);
		      }//iter 1 o/p: output3, iter 1 i/p: output1, iter 8 o/p:outiternext
	      } 
	    }
	    
	     public static void deleteDir(String uri) throws IOException {
             Configuration config = new Configuration();
             FileSystem hdfs = FileSystem.get(config);
             Path path = new Path(uri);
             hdfs.delete(path, true);
     }
     
	}
	