package query_3;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;

import java.io.IOException;


public class GPPercentReducer extends Reducer<Text, Text, Text, FloatWritable> 
{
	private FloatWritable total_GP = new FloatWritable();
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException
	{
		float totalSales = 0;
		float totalGP = 0;
		float totalGPPercent, sales, grossProfit;
		
		for(Text val:values)
		{
			String[] line = val.toString().split(";");
			sales = Float.parseFloat(line[0]);
			grossProfit = Float.parseFloat(line[1]);
			
			totalSales += sales;
			totalGP += grossProfit;
		}
		
		totalGPPercent = (totalGP / totalSales) * 100;
		
		total_GP.set(totalGPPercent);
		
		context.write(key, total_GP);
	}
}
