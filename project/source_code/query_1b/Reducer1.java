
import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class Reducer1 extends Reducer<Text, IntWritable, Text, Text> 
{
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException
	{
		int ct2011 = 0, ct2012 = 0, ct2013 = 0, ct2014 = 0, ct2015 = 0, ct2016 = 0;
		int year;
		
		for(IntWritable val:values)
		{
			year = val.get();
			
			switch(year)
			{
				case 2011: ct2011++;
						break;
						 
				case 2012: ct2012++;
						break;
						
				case 2013: ct2013++;
						break;
					
				case 2014: ct2014++;
						break;
				
				case 2015: ct2015++;
						break;
				
				case 2016: ct2016++;
						break;
			}
		}
		
		
		if((ct2011 != 0) && (ct2012 != 0) && (ct2013 != 0) && (ct2014 != 0) && (ct2015 != 0) && (ct2016 != 0))
		{
			String allcount = ct2011 + "\t" + ct2012 + "\t" + ct2013 + "\t" + ct2014 + "\t" + ct2015 + "\t" + ct2016;
			
			context.write(key, new Text(allcount));
		}
	}
}
