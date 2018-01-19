import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class Mapper1 extends Mapper<LongWritable, Text, IntWritable, IntWritable>
{
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String record[] = value.toString().split("\t");
		
		int year = Integer.parseInt(record[7]);
		String job_title = record[4];
		
		if(job_title.contains("DATA ENGINEER") == true)
		{
			context.write(new IntWritable(year), new IntWritable(1));
		}
	}
}
