import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

	public class MapSorted extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, Text> {  
		private Text word = new Text();
//		private static int N;
//		public void configure(JobConf job) {
//		    N = job.getInt("N", 0);
//		}
		public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
			String []values = value.toString().split("\t");
			Double val = Double.valueOf(values[1]);
			//PageRank.N=215549;//remove the hardcoding
			
			String line2=null;
			try{
                Path pt=new Path("s3n://pagerank12/PageRank.n.out/part-00001");
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                
                line2=br.readLine();

        }catch(Exception e){
        }
			 String[] tokens = line2.split("=");
			 
				//int n = Integer.valueOf(line2);
				 int n = Integer.valueOf(tokens[1]);
			if(val>5.0/n)
			{
				output.collect(new DoubleWritable(val), new Text(values[0]));
			}
		}

	}