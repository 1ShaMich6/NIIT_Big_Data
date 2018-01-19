
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class TSMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private final static IntWritable one = new IntWritable(1);
	private Text sentence = new Text();
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String mySearchText = context.getConfiguration().get("myText");
		String line = value.toString();
		String newline = line.toLowerCase();
		String newText = mySearchText.toLowerCase();
		
		if(mySearchText != null)
		{
			if(newline.contains(newText))
			{
				sentence.set(newline);
				context.write(sentence, one);
			}
		}
	}
}
