
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
//import org.jdom.*;
//import org.jdom.input.*;
//import org.jdom.JDOMException;


	public class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			StringBuilder result = new StringBuilder();				//Strings''
			Set<String> allValues = new HashSet<String>();
			
			while (values.hasNext()) {
				allValues.add(values.next().toString());//
//				result.append(new Text(values.next()));
//				result.append("\t");
			}
			for(String setValue:allValues)
			{
			result.append(new Text(setValue));
			result.append("\t");
			}
			output.collect(key, new Text(new String(result)));
		}
	}