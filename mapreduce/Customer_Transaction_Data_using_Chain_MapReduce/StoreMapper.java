import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;


public class StoreMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String[] parts = value.toString().split(",");
		String custId = parts[2];
		String amount = "s" + "|" + parts[3];
		
		context.write(new Text(custId), new Text(amount));
	}
}
