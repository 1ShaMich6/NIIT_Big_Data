package query_5;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

public class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable>
{
	int count = 1;
	
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException
	{
		
		for(Text val:values)
		{
			if(count <= 5)
			{
				String[] part = val.toString().split("\\|");
//				String category = part[1];
				String product = part[1];
				
//				context.write(new Text(category), key);
				context.write(new Text(product), key);
			}
		}
		count++;
	}
}
