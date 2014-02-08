
<<<<<<< HEAD
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
=======
import java.io.IOException;
>>>>>>> 2cffc36c614f87eb3523fbed9c14a3025c7a5ceb
import java.io.Reader;
import java.io.StringReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

<<<<<<< HEAD
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
public class PageRank {
	public static void main(String[] args) throws Exception {
		/*Job1*/
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("PageRank");//job name
		conf.setInputFormat(XmlInputFormat.class); 
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		conf.set("mapred.textoutputformat.separator", "\t");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path("s3n://pagerank12/tmp1"));
		JobClient.runJob(conf);
		Path f = new Path("s3n://pagerank12/PageRank.inlink.out");
		FileSystem.get(conf).create(f);
		FileUtil.copyMerge(FileSystem.get(conf), new Path("s3n://pagerank12/tmp1"),FileSystem.get(conf), new Path("s3n://pagerank12/PageRank.inlink.out"), true, conf,"");
		
		/*Job2*/
		JobConf conf2 = new JobConf(PageRank.class);
		conf2.setJobName("PageRankPages");//job name
		conf2.setInputFormat(XmlInputFormat.class); 
		conf2.set("xmlinput.start", "<page>");
		conf2.set("xmlinput.end", "</page>");
		conf2.set("mapred.textoutputformat.separator", "\t");
		conf2.setOutputKeyClass(Text.class); //map
		conf2.setOutputValueClass(IntWritable.class); //map
		conf2.setMapperClass(MapPages.class);
		conf2.setReducerClass(ReducePages.class);
		conf2.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf2, new Path(args[0]));//change input
		FileOutputFormat.setOutputPath(conf2, new Path("s3n://pagerank12/tmp2"));
		JobClient.runJob(conf2);
		f = new Path("s3n://pagerank12/PageRank.n.out");
		FileSystem.get(conf).create(f);
		FileUtil.copyMerge(FileSystem.get(conf), new Path("s3n://pagerank12/tmp2"),FileSystem.get(conf), new Path("s3n://pagerank12/PageRank.n.out"), true, conf,"");
		
		/*Job3*/
		String inputDir ="s3n://pagerank12/PageRank.inlink.out"; 
		String outputDir1="s3n://pagerank12/PageRank.iter0.dir";//output iter 1 
		for(int i=0;i<2;i++)//change to 8, and 1 to 8
		{
			JobConf conf3 = new JobConf(PageRank.class);
			conf3.setJobName("PageRankIterative"+i);//job name
			conf3.setMapperClass(MapIterative.class);
			conf3.setReducerClass(ReduceIterative.class);
			conf3.set("mapred.textoutputformat.separator", "\t");
			conf3.setInputFormat(TextInputFormat.class);       
			conf3.setOutputKeyClass(Text.class);
			conf3.setOutputValueClass(Text.class);
			conf3.setOutputFormat(TextOutputFormat.class);
			FileInputFormat.setInputPaths(conf3, new Path(inputDir));//initial input is the output of Part 1, with pagerank of 1 included per line, (also included N as static)
			FileOutputFormat.setOutputPath(conf3, new Path(outputDir1));

			JobClient.runJob(conf3);
			String tmp = inputDir;
			FileUtil.copyMerge(FileSystem.get(conf), new Path("s3n://pagerank12/PageRank.iter"+i+".dir"),FileSystem.get(conf), new Path("s3n://pagerank12/PageRank.iter"+i+".out"), true, conf,"");
			outputDir1= "s3n://pagerank12/PageRank.iter"+(i+1)+".dir";//now a file,  give diff names above? file created?
			inputDir="s3n://pagerank12/PageRank.iter"+i+".out";
		}
		FileUtil.copyMerge(FileSystem.get(conf), new Path("s3n://pagerank12/PageRank.iter7.dir"),FileSystem.get(conf), new Path("s3n://pagerank12/PageRank.iter7.out"), true, conf,"");
		
		/*Job4*/
		JobConf conf4 = new JobConf(PageRank.class);
		conf4.setJobName("PageRankSorted");//job name
		conf4.setInputFormat(TextInputFormat.class); 
		conf4.setOutputKeyClass(DoubleWritable.class); //map
		conf4.setOutputValueClass(Text.class); //map
		conf4.set("mapred.textoutputformat.separator", "\t");
		conf4.setOutputKeyComparatorClass(LongWritable.DecreasingComparator.class);
		conf4.setMapperClass(MapSorted.class);
		conf4.setReducerClass(ReduceSorted.class);
		conf4.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf4, new Path("s3n://pagerank12/PageRank.iter7.out"));
		FileOutputFormat.setOutputPath(conf4, new Path("s3n://pagerank12/PageRank.sorted.out"));
		JobClient.runJob(conf4);
	}

public static void deleteDir(String uri) throws IOException {
	Configuration config = new Configuration();
	FileSystem hdfs = FileSystem.get(config);
	Path path = new Path(uri);
	hdfs.delete(path, true);
}
=======
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.jdom.*;
import org.jdom.input.*;
import org.jdom.JDOMException;

public class PageRank {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {  
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
				Pattern p1 = Pattern.compile("\\[\\[([^\\]^\\[]+)\\]\\]");
				Matcher m1 = p1.matcher(allText);
				while(m1.find())
				{
					String textValue=m1.group(1);
					if(textValue.contains("|"))
					{
						textValue= textValue.substring(0, textValue.indexOf('|'));
					}
					output.collect(new Text(title), new Text(textValue));
				}
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
		conf.setReducerClass(Reduce.class);

		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
>>>>>>> 2cffc36c614f87eb3523fbed9c14a3025c7a5ceb
}
