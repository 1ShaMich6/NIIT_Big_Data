package query_6;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;

public class SortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>
{
	int count = 1;
	
	@Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException
	{
		
		for(Text val:values)
		{
			if(count <= 5)
			{
				String[] part = val.toString().split("\\|");
				String product = part[1];
				
				context.write(new Text(product), key);
			}
		}
		count++;
	}
}

