
import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


public class Reducer1 extends Reducer<Text, Text, Text, Text> 
{
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException
	{
		int certified = 0, certified_withdrawn = 0, denied = 0, withdrawn = 0, total = 0;
		
		for(Text val:values)
		{
			String case_status = val.toString();
			
			//Total count
			total++;
			
			//Count according to case_status
			if(case_status.equals("CERTIFIED"))
				certified++;
			else if(case_status.equals("CERTIFIED-WITHDRAWN"))
				certified_withdrawn++;
			else if(case_status.equals("WITHDRAWN"))
				withdrawn++;
			else
				denied++;
		}
		
		String outputValue = certified + "\t" + certified_withdrawn + "\t" + withdrawn + "\t" + denied + "\t" + total;
		
		context.write(key, new Text(outputValue));
	}
}
