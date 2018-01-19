import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;


public class SalesMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String record = value.toString();
		String[] line = record.split(",");
		String prodId = new String(line[0]);
		String qtySold = new String("s" + "|" + line[1]);
		
		context.write(new Text(prodId), new Text(qtySold));
	}
}
