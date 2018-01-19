import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class AvgMapper extends Mapper<LongWritable, Text, Text, Text>
{
	long old_count = 0;
	boolean first_record = true;
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String record[] = value.toString().split("\t");
		
		String year = record[0];
		
		if(first_record == true)
		{
			long new_count = Long.parseLong(record[1]);
			String outputValue = record[1];
			
			old_count = new_count;
			first_record = false;
			context.write(new Text(year), new Text(outputValue));
		}
		
		else
		{
			long new_count = Long.parseLong(record[1]);			
			double growth_percent = (((double)new_count - (double)old_count) / (double)old_count) * 100;
			
			String outputValue = new_count + "\t" + growth_percent;
			old_count = new_count;
			
			context.write(new Text(year), new Text(outputValue));
		}
	}
}
