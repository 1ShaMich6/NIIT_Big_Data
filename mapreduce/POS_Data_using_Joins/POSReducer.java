import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class POSReducer extends Reducer<Text, Text, Text, IntWritable> 
{
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException
	{
		int totalQty = 0, qty;
		
		for(Text val:values)
		{
			String[] line = val.toString().split(",");
			qty = Integer.parseInt(line[0]);
			
			totalQty += qty;
		}
		
		context.write(key, new IntWritable(totalQty));
	}
}
