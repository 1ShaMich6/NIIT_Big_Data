
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;


public class PhoneMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
{
	Text phone_no = new Text();
	IntWritable inMinutes = new IntWritable();
	
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String caller_id, callStartTime, callEndTime;
		long callDuration, durationInMinutes;
		
		try
		{
			String[] record = value.toString().split(",");
			
			if(record[4].equalsIgnoreCase("1") == true)   //if it is a long distance call
			{
				caller_id = record[0];
				callStartTime = record[2];
				callEndTime = record[3];
				
				callDuration = toMillis(callEndTime) - toMillis(callStartTime);
				durationInMinutes = callDuration / (1000 * 60);
				
				phone_no.set(caller_id);
				inMinutes.set((int)durationInMinutes);
				
				context.write(phone_no, inMinutes);
			}
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
	
	
	private long toMillis(String str)
	{
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dt = null;
		
		try 
		{
			dt = format.parse(str);
		} 
		catch (ParseException e) 
		{
			e.printStackTrace();
		}
		
		return dt.getTime();
	}
}
