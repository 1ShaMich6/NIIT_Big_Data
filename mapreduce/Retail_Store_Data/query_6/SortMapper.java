package query_6;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;


public class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> 
{
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException 
	{
		String[] line = value.toString().split("\t");
		
		double totalLoss = Double.parseDouble(line[0]);
		String ageProduct = line[1];
		
		context.write(new DoubleWritable(totalLoss), new Text(ageProduct));
	}
}
