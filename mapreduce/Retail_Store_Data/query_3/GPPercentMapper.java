package query_3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class GPPercentMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String[] record = value.toString().split(";");
		
//		String prodId = record[5];
		String Category = record[4];
		float cost = Float.parseFloat(record[7]);
		float sales = Float.parseFloat(record[8]);
		
		float grossProfit = sales - cost;
		String myData = new String(sales + "; " + grossProfit);
		
//		context.write(new Text(prodId), new Text(myData));
		context.write(new Text(Category), new Text(myData));
	}
}
