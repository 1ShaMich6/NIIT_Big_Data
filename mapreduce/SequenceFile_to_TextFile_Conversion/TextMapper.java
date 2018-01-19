import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class TextMapper extends Mapper<LongWritable, Text, LongWritable, Text> 
{
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException 
	{
		context.write(key, value);
	}
}
