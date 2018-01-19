package query_2;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;

import java.io.IOException;

public class GPReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>
{
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
			throws IOException, InterruptedException
	{
		float totalGP = 0;
		
		for(FloatWritable val:values)
		{
			totalGP += val.get();
		}
		
		context.write(new Text(key), new FloatWritable(totalGP));
	}
}
