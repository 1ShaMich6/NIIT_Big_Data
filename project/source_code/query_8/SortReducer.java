
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;


public class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> 
{
	
	@Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException
	{
		DecimalFormat deciFormat = new DecimalFormat("###########.##");
		
		for(Text val:values)
		{
			double avg_wage = key.get();
			String avg_wage_format = deciFormat.format(avg_wage);
			
			context.write(val, new Text(avg_wage_format));
		}
	}
}
