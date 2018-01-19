package query_5;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

public class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> 
{
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException 
	{
		String[] line = value.toString().split("\t");
		
//		double totalProfit = Double.parseDouble(line[0]);
		int totalProfit = Integer.parseInt(line[0]);
//		String ageCategory = line[1];
		String ageProduct = line[1];
		
		context.write(new IntWritable(totalProfit), new Text(ageProduct));
	}
}
