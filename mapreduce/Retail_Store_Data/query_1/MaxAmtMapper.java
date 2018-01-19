package query_1;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Date;

public class MaxAmtMapper extends Mapper<LongWritable, Text, Text, Text>
{
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String dataText = null;
		String[] record = value.toString().split(";");
		
		SimpleDateFormat fr = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		Date dt = new Date();
		
		try
		{
			dt = fr.parse(record[0]);
		}
		catch(ParseException e)
		{
			System.out.print(e.getMessage());
		}
		
		fr = new SimpleDateFormat("yyyy-MM-dd");
		dataText = fr.format(dt);

//		dataText.concat("; " + record[1] + "; " + record[8]); -----> doesn't work !!!
		dataText = dataText + "; " + record[1] + "; " + record[8];
		
		context.write(new Text("common"), new Text(dataText));
	}
}
