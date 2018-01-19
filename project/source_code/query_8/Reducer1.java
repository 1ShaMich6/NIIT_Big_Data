
import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;


public class Reducer1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> 
{
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException
	{
		int count = 0;
		double total_wage = 0.0, average_wage = 0.0;
		
		for(DoubleWritable val:values)
		{
			double wage = val.get();
			
			total_wage += wage;
			
			count++;
		}
		
		average_wage = total_wage/count;
		
		context.write(key, new DoubleWritable(average_wage));
	}
}
