import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


public class AvgReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>
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
				String job_title = val.toString();
				double avg_growth = key.get();
				
				DecimalFormat deciFormat = new DecimalFormat("######.####");
				String outputValue = deciFormat.format(avg_growth);
				avg_growth = Double.parseDouble(outputValue);
				
				context.write(new Text(job_title), new DoubleWritable(avg_growth));
			}
		}
		
		count++;
	}
}
