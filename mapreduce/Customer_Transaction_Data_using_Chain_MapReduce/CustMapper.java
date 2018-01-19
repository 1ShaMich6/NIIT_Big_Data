import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;


public class CustMapper extends Mapper<LongWritable, Text, Text, Text>
{
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String[] parts = value.toString().split(",");
		
		String custId = parts[0];
		String profession = "c" + "|" + parts[4];
		
		context.write(new Text(custId), new Text(profession));
	}
}
