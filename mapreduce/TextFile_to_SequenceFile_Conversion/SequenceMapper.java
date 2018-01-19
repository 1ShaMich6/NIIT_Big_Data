
import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class SequenceMapper extends Mapper<LongWritable, Text, LongWritable, Text> 
{
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
//		String record = value.toString();
//		String[] parts = record.split(",");
//		
//		long myKey = Long.parseLong(parts[0]);
		
//		context.write(new LongWritable(myKey), new Text(parts[1]));
		context.write(key, value);
	}
}
