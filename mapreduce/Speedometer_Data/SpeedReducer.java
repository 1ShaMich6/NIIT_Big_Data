
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import java.io.IOException;


public class SpeedReducer extends Reducer<Text, IntWritable, Text, FloatWritable>
{
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException
	{
		
		int total_ct = 0;
		int offence_ct = 0;
		int temp_val = 0;
		float offence_percent;
		
		for(IntWritable value:values)
		{
			total_ct++;
			temp_val = value.get();
			if(temp_val == 1)
				offence_ct++;
		}
		
		offence_percent = (( (float)offence_ct / (float)total_ct) * 100);
//		offence_percent = ((float)(offence_ct / total_ct) * 100);   -----> doesn't work correctly!
//											------> gives a wrong output
//		
//		offence_percent = ((offence_ct*100) / total_ct);   ----> gives the correct output
		
		context.write(key, new FloatWritable(offence_percent));
	}
}
