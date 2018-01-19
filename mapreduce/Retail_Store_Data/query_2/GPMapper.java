package query_2;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;

import java.io.IOException;


public class GPMapper extends Mapper<LongWritable, Text, Text, FloatWritable> 
{
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String ProdId;
//		String Category;
		float gross_profit, sales, cost;
		
		String[] record = value.toString().split(";");
		ProdId = record[5];
//		Category = record[4];
		cost = Float.parseFloat(record[7]);
		sales = Float.parseFloat(record[8]);
		
		gross_profit = sales - cost;
		
		context.write(new Text(ProdId), new FloatWritable(gross_profit));
//		context.write(new Text(Category), new FloatWritable(gross_profit));
	}
}