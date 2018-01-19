package query_4;

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
		String product = record[5];
		double salesAmt = Double.parseDouble(record[8]);
		
		context.write(new Text(product), new DoubleWritable(salesAmt));
	}
}
