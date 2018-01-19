package query_4;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;


public class SortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> 
{
	int count = 0;
	
	@Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException
	{
		for(Text val:values)
		{
//			if(count < 4)   // top 4 ---- since count = 0 initially
			if(count < 10)
			{
				context.write(val, key);
			}
		}
		
		count++;
	}
}
