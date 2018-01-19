import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class Mapper1 extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String record[] = value.toString().split("\t");
		
		String required_position = context.getConfiguration().get("fullTimePosition");
		String required_year = context.getConfiguration().get("yearFilter");
		
		String case_status = record[1];
		String job_title = record[4];
		String full_time_position = record[5];
		double prevailing_wage = Double.parseDouble(record[6]);
		String year = record[7];
		
		if(case_status.equals("CERTIFIED") || case_status.equals("CERTIFIED-WITHDRAWN"))
		{
			//If user has asked for all the years
			if(full_time_position.equals(required_position) && required_year.toLowerCase().equals("all"))
			{
				String outputKey = job_title + "\t" + year + "\t" + full_time_position;
				
				context.write(new Text(outputKey), new DoubleWritable(prevailing_wage));
			}
			
			//if user has asked for a specific year
			else if(full_time_position.equals(required_position) && year.equals(required_year))
			{
				String outputKey = job_title + "\t" + year + "\t" + full_time_position;
				
				context.write(new Text(outputKey), new DoubleWritable(prevailing_wage));
			}
			
			else
			{
				//do nothing
			}
		}
	}
}