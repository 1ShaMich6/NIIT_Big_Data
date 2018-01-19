import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>
{
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String record[] = value.toString().split("\t");
		
		String outputValue = record[0] + "\t" + record[1] + "\t" + record[2];
		double average_wage = Double.parseDouble(record[3]);
		
		context.write(new DoubleWritable(average_wage), new Text(outputValue));
	}
}