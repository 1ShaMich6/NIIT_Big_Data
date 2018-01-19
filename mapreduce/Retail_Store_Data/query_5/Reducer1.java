package query_5;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;


public class Reducer1 extends Reducer<Text, IntWritable, IntWritable, Text>
{
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException 
	{
//		double totalProfit = 0.0, amt;
		int totalProfit = 0, amt;
		
		for(IntWritable val:values)
		{
			amt = val.get();
			totalProfit += amt;
		}
		
		if(totalProfit > 0)
		{
			context.write(new IntWritable(totalProfit), key);
		}
	}
}
