
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


public class AvgReducer extends Reducer<Text, Text, Text, Text> 
{
	double total = 0.0;
	boolean first_record = true;
	int count = 0;
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException
	{
		if(first_record == true)
		{
			first_record = false;
			
			for(Text val:values)
			{
				context.write(key, val);
			}
			
		}
		
		else
		{
			for(Text val:values)
			{
				String line[] = val.toString().split("\t");
				double growth = Double.parseDouble(line[1]);
				
				total += growth;
				count++;
				
				DecimalFormat deciformat = new DecimalFormat("###.#####");
				
				String outputValue = line[0] + "\t" + deciformat.format(growth);
				
				context.write(key, new Text(outputValue));
			}
		}
	}
	
	
	@Override
	public void cleanup(Context context) 
			throws IOException, InterruptedException
	{
		DecimalFormat deciformat = new DecimalFormat("####.###");
		
		double avg_growth = total / count;
		String finalOutput = deciformat.format(avg_growth);
		String comment = "\nAverage Growth (2011-2016):  ";
		
		context.write(new Text(comment), new Text(finalOutput));
	}
}
