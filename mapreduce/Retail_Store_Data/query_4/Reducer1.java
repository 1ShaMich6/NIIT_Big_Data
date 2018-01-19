package query_4;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


public class Reducer1 extends Reducer<Text, DoubleWritable, DoubleWritable, Text> 
{
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException
	{
		double totalAmt = 0.0, amt;
		
		for(DoubleWritable val:values)
		{
			amt = val.get();
			totalAmt += amt;
		}
		
		context.write(new DoubleWritable(totalAmt), key);
	}
}
