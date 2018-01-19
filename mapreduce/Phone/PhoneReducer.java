
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;


public class PhoneReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
	IntWritable totalDur = new IntWritable();
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException
	{
		int totalDuration = 0;
		int temp;
		
		for(IntWritable value:values)
		{
			temp = value.get();
			totalDuration += temp;
		}
		
		if(totalDuration > 60)
		{
			totalDur.set(totalDuration);
			context.write(key, totalDur);
		}
	}
}
