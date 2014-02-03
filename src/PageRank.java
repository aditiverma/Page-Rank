
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
	
	public class PageRank {
		public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {  
	//	private final static IntWritable one = new IntWritable(1);
	      private Text word = new Text();
	      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    	  String xmlString = value.toString();
	    	  SAXBuilder builder = new SAXBuilder();
	            Reader in = new StringReader(xmlString);
	            String value1="";
	            Document doc;
				try {
				doc = builder.build(in);
				Element root = doc.getRootElement();
				String title=""; String allText="";
				if(root.getChild("title")!=null)
				{
					 title =root.getChildText("title");//later replace spaces by _ //changed from getchild.gettext to this function 
					 title = title.replaceAll(" ", "_");
				}
				else
				{
					 title="no title";
				}
				if(root.getChild("revision")!=null)
				{
	             allText =root.getChild("revision").getChildText("text");//this is actually inside <revision>
	             allText=allText.replaceAll(" ", "_");//must assign here
				}
				else
				{
				 allText="no text [[notext]] ";
				}
	            //Pattern p1 = Pattern.compile("^.*(\\s\\[\\[[a-zA-Z0-9_ ]+\\]\\]\\s).*$");
				//Pattern p1 = Pattern.compile("^\\[\\[[a-zA-Z0-9_ ]+\\]\\]$");
				//Pattern p1 = Pattern.compile("\\[\\[(.*?)\\]\\]");
				Pattern p1 = Pattern.compile("\\[\\[([^\\]^\\[]+)\\]\\]");
	    //        Pattern p2 = Pattern.compile("^\\s\\[\\[[a-zA-Z0-9_ \\|]+\\]\\]\\s$");//PAttern matched twice!
	            Matcher m1 = p1.matcher(allText);
	          //  Matcher m2 = p2.matcher(allText);
	            while(m1.find())
	             {
	            String textValue=m1.group(1);
	            if(textValue.contains("|"))
	            {
	            	textValue= textValue.substring(0, textValue.indexOf('|'));
	            }
	            output.collect(new Text(title), new Text(textValue));
	          	//output.collect(new Text(title), new Text(allText));
	             }
//	            while(m2.find())
//	             {
//	            	output.collect(new Text(title), new Text(m2.group()));
//	            	//output.collect(new Text("abc2 title"), new Text("abc2 text") );
//	             }
				} catch (JDOMException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				catch(NullPointerException e)
				{
					e.printStackTrace();	
				}
	      }
	        
	    }
	
	    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	    	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    	StringBuilder result = new StringBuilder();				//Strings''
	        while (values.hasNext()) {
	        	result.append(new Text(values.next()));
	        	result.append("\t");
	        }
	       // output.collect(new Text("abc3 title"), new Text("abc3 text") );
	        output.collect(key, new Text(new String(result)));
	      }
	    }
	
	    public static void main(String[] args) throws Exception {
	      JobConf conf = new JobConf(PageRank.class);
	      conf.setJobName("PageRank");//job name
	
	      conf.setInputFormat(XmlInputFormat.class); 
	      conf.set("xmlinput.start", "<page>");
	      conf.set("xmlinput.end", "</page>");
	      
	      conf.setOutputKeyClass(Text.class);
	      conf.setOutputValueClass(Text.class);
	      
	      conf.setMapperClass(Map.class);
	   //  conf.setCombinerClass(Reduce.class);
	      conf.setReducerClass(Reduce.class);
	      
	      conf.setOutputFormat(TextOutputFormat.class);
	    //  System.out.println("no. of map tasks"+conf.getNumMapTasks());-different from no. of times map() is called
	      FileInputFormat.setInputPaths(conf, new Path(args[0]));
	      FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	      JobClient.runJob(conf);
	    }
	}
	