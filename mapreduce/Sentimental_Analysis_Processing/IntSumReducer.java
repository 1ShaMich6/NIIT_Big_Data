import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;


public class IntSumReducer extends Reducer<Text, IntWritable, NullWritable, Text> 
{
	int pos_total = 0;
	int neg_total = 0;
	double sentPercent = 0.00;
	
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException
	{
		int sum = 0;
		
		for(IntWritable val:values)
		{
			sum += val.get();
		}
		
		if(key.toString().equals("positive"))
		{
			pos_total = sum;
		}
		
		if(key.toString().equals("negative"))
		{
			neg_total = sum;
		}
	}
	
	
	protected void cleanup(Context context) 
			throws IOException, InterruptedException
	{
		double num = ((double)pos_total) - ((double)neg_total);
		double den = ((double)pos_total) + ((double)neg_total);
		sentPercent = (num / den) * 100;
		
		String msg = "Sentiment percent for the given text is " + String.format("%f", sentPercent);
		
		context.write(NullWritable.get(), new Text(msg));
	}
}
