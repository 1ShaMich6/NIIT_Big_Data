import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;


public class Mapper2 extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException 
	{
		String[] parts = value.toString().split("\t");
		String profession = parts[0];
		double amount = Double.parseDouble(parts[1]);
		
		context.write(new Text(profession), new DoubleWritable(amount));
	}
}
