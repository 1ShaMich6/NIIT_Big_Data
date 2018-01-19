
import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable>
{
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String record[] = value.toString().split("\t");
		
		int year = Integer.parseInt(record[7]);
		String job_title = record[4];
		
		context.write(new Text(job_title), new IntWritable(year));
	}
}
