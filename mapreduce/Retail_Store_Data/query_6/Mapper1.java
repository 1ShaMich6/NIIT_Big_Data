package query_6;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;


public class Mapper1 extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String[] record = value.toString().split(";");
		
		String age = record[2];
//		String category = record[4];
		String product = record[5];
		double totalCost = Double.parseDouble(record[7]);
		double totalSales = Double.parseDouble(record[8]);
		
		double loss = totalSales - totalCost;
//		String temp = age + "|" +product;
		
//		String temp = (age.toCharArray())[0] + "|" +category;
		String temp = (age.toCharArray())[0] + "|" +product;
		
		context.write(new Text(temp), new DoubleWritable(loss));
		
	}
}