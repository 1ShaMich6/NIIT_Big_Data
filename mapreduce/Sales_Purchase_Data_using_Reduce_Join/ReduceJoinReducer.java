import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;


public class ReduceJoinReducer extends Reducer<Text, Text, Text, Text>
{
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException
	{
		int totalSalesQty = 0, totalPurchaseQty = 0;
		String idTag = null;
		
		for(Text val:values)
		{
//			String[] part = val.toString().split("|");  // Wrong usage !!!!
			String[] part = val.toString().split("\\|");
			idTag = part[0];
						
			if(idTag.equals("p"))
			{
				totalPurchaseQty += Integer.parseInt(part[1]);
			}	
			else if(idTag.equals("s"))
			{
				totalSalesQty += Integer.parseInt(part[1]);
			}	
		}
		
		String temp = String.format("%d\t%d", totalPurchaseQty, totalSalesQty);
		context.write(key, new Text(temp));
	}
}
