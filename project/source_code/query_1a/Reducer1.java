
import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;

public class Reducer1 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> 
{
	
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException
	{
		int count = 0;
		
		for(IntWritable val:values)
		{
			count += val.get();
		}
		
		context.write(key, new IntWritable(count));
	}
}
