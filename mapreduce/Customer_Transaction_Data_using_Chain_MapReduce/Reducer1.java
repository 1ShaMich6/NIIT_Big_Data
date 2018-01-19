import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;


public class Reducer1 extends Reducer<Text, Text, Text, DoubleWritable> 
{
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException
	{
		double total = 0.0;
		String profession = "unknown";
		
		for(Text val:values)
		{
			String[] part = val.toString().split("\\|");
			String identifier = part[0];
			
			if(identifier.equals("s"))
			{
				double amt = Double.parseDouble(part[1]);
				//here, we calculate the total amount of each CUSTOMER ID 
				//But, we extract the customer's profession rather than his/her customer ID
				total += amt;
			}
			else if(identifier.equals("c"))
			{
				profession = part[1];
			}
		}
		
		context.write(new Text(profession), new DoubleWritable(total));
	}
}
