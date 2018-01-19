
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class EmpReducer extends Reducer<Text, Text, Text, IntWritable>
{
	public int max = 0;
	private Text outputKey = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException
	{
		max = 0;
		
		for(Text val:values)
		{
//			outputKey.set(key);
			String[] str = val.toString().split(",");
			if(Integer.parseInt(str[4]) > max)
			{
				max = Integer.parseInt(str[4]);
				String mykey = str[3] + ',' + str[1] + ',' + str[2];
				outputKey.set(mykey);
			}
		}
		
		context.write(outputKey, new IntWritable(max));
	}
}
