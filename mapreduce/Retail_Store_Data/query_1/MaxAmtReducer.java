package query_1;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class MaxAmtReducer extends Reducer<Text, Text, Text, Text> 
{
	private Text cDate = new Text();
	private Text cId = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException
	{
		String maxCid = null;
		String maxDate = null;
		float maxAmt = 0, amt;
		
		for(Text val:values)
		{
			String[] line = val.toString().split(";");
			amt = Float.parseFloat(line[2]);
			
			if(amt > maxAmt)
			{
				maxAmt = amt;  // very very very very IMPORTANT LINE !!
				
				maxDate = line[0];
				maxCid = line[1];
			}
		}
		
		cDate.set(maxDate);
		cId.set(maxCid);
		
		context.write(cDate, cId);
	}
}
