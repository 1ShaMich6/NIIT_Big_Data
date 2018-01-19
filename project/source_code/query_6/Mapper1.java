import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text>
{
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String required_year = context.getConfiguration().get("yearFilter");
		
		String record[] = value.toString().split("\t");
		
		String case_status = record[1];
		String year = record[7];
		
		//When user chooses "ALL"
		if(required_year.toLowerCase().equals("all"))
		{
			context.write(new Text(year), new Text(case_status));
		}
		
		//When user required for a specific year
		else if(year.equals(required_year))
		{
			context.write(new Text(year), new Text(case_status));
		}
		
		else
		{
			//do nothing
		}
	}
}