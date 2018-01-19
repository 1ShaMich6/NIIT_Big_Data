package query_4;

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
		String[] record = value.toString().split("\t");
		double totalSalesAmt = Double.parseDouble(record[0]);
		String product = record[1];
		
		context.write(new DoubleWritable(totalSalesAmt), new Text(product));
	}
}
