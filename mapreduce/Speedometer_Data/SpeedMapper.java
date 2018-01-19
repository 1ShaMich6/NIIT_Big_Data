
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;


public class SpeedMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
{
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		try
		{
			String[] record = value.toString().split(",");
			
			String carNum = record[0];
			float speed = Float.valueOf(record[1]);
			int offence_check = 0;
			
			if(speed > 65)
				offence_check = 1;
			
			context.write(new Text(carNum), new IntWritable(offence_check));
		}
		
		catch(Exception e)
		{
			
		}
	}
}
