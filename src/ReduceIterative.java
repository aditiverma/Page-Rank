import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ReduceIterative extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
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
					isValidLink = true;//check red links
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

			//PageRank.N=5;
			String line2=null;
			try{
                Path pt=new Path("s3n://pagerank12/PageRank.n.out/part-00001");
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                
                line2=br.readLine();

        }catch(Exception e){
        }
			 String[] tokens = line2.split("=");
				 int n = Integer.valueOf(tokens[1]);
			
			sumRank = sumRank*.85 +.15/n;
			outValue.set(String.valueOf(sumRank) +"\t"+ outLinks);
			if(isValidLink)
			{
				output.collect(key, outValue);
			}
		}
	}
