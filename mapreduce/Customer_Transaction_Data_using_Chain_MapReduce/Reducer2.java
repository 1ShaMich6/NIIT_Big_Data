import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;


public class Reducer2 extends Reducer<Text, DoubleWritable, DoubleWritable, Text> 
{
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException
	{
		double totalOfProfession = 0.0;
		
		for(DoubleWritable val:values)
		{
			totalOfProfession += val.get();
		}
		
		//reversal of key-value pair
		context.write(new DoubleWritable(totalOfProfession), key);
	}
}
