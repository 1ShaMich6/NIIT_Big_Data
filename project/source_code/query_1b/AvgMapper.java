
import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class AvgMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>
{
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String record[] = value.toString().split("\t");
		
		String job_title = record[0];
		
		double ct2011 = Double.parseDouble(record[1]);
		double ct2012 = Double.parseDouble(record[2]);
		double ct2013 = Double.parseDouble(record[3]);
		double ct2014 = Double.parseDouble(record[4]);
		double ct2015 = Double.parseDouble(record[5]);
		double ct2016 = Double.parseDouble(record[6]);
		
		
		double growth1 = (ct2012 - ct2011)/ct2011 * 100;
		double growth2 = (ct2013 - ct2012)/ct2012 * 100;
		double growth3 = (ct2014 - ct2013)/ct2013 * 100;
		double growth4 = (ct2015 - ct2014)/ct2014 * 100;
		double growth5 = (ct2016 - ct2015)/ct2015 * 100;
		
		double avg_growth = (growth1 + growth2 + growth3 + growth4 + growth5)/5;
		
		context.write(new DoubleWritable(avg_growth), new Text(job_title));
	}
}
