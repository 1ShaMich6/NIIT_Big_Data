import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;


public class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> 
{
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String[] parts = value.toString().split("\t");
		double amount = Double.parseDouble(parts[0]);
		String profession = parts[1];
		
		context.write(new DoubleWritable(amount), new Text(profession));
	}
}
