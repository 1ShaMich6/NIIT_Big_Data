import java.util.StringTokenizer;
import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
{
	//because the default count of each word is 1
	private final static IntWritable one = new IntWritable(1);
	
	private Text word = new Text();
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		StringTokenizer itr = new StringTokenizer(value.toString());
		
		while(itr.hasMoreTokens())
		{
			String myword = itr.nextToken().toLowerCase();
			word.set(myword);
			context.write(word, one);
//			context.write(word, new IntWritable(1));
		}
	}
}
