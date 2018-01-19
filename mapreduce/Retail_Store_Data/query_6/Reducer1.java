package query_6;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;


public class Reducer1 extends Reducer<Text, DoubleWritable, DoubleWritable, Text>
{
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException 
	{
		double totalLoss = 0.0, amt;
		
		for(DoubleWritable val:values)
		{
			amt = val.get();
			totalLoss += amt;
		}
		
		if(totalLoss < 0)
		{
			context.write(new DoubleWritable(totalLoss), key);
		}
	}
}
