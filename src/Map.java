import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
//import org.jdom.Document;
//import org.jdom.Element;
//import org.jdom.JDOMException;
//import org.jdom.input.SAXBuilder;

public  class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {  
		private Text word = new Text();
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String xmlString = value.toString();
			xmlString = xmlString.replace("\n", "").replace("\r", "");
			//SAXBuilder builder = new SAXBuilder();
			Reader in = new StringReader(xmlString);
			System.out.println("XML STRING IS "+xmlString);
			String value1="";
			//Document doc;
			//String s = "<B>Test</B>";
	        String regtitle = "\\<title(.*)\\<\\/title\\>";
	        String title=""; 
	        Pattern p = Pattern.compile(regtitle);
	    	Matcher m = p.matcher(xmlString);//xmlString
			while(m.find())
			{
				title=m.group(1);
				title = title.replaceAll(" ", "_");
			}
			
			 String regtext = "\\<text(.*)\\<\\/text\\>";
			 String alltext="";
		        Pattern p1 = Pattern.compile(regtext);
		    	Matcher m1 = p1.matcher(xmlString);
				while(m1.find())
				{
					alltext=m1.group();
					
				}
			
				Pattern p2 = Pattern.compile("\\[\\[([^\\]^\\[]+)\\]\\]");
				Matcher m2 = p2.matcher(alltext);
				//Matcher m2 = p2.matcher("<text>[[aaa]][[bbb]]</text>");
				while(m2.find())
				{
					String textValue=m2.group(1);
					if(textValue.contains("|"))
					{
						textValue= textValue.substring(0, textValue.indexOf('|'));
						textValue = textValue.replaceAll(" ", "_");
					}
					output.collect(new Text(title), new Text(textValue));
					//output.collect(new Text(xmlString), new Text(textValue));
				}
				
			}

		}

