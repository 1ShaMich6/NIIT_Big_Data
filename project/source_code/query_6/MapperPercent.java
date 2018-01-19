import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class MapperPercent extends Mapper<LongWritable, Text, Text, Text>
{
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String record[] = value.toString().split("\t");
		
		String year = record[0];
		
		long certified = Long.parseLong(record[1]);
		long certified_withdrawn = Long.parseLong(record[2]);
		long withdrawn = Long.parseLong(record[3]);
		long denied = Long.parseLong(record[4]);
		long total = Long.parseLong(record[5]);
		
		double c_percent = ((double)certified / (double)total) * 100;
		double cw_percent = ((double)certified_withdrawn / (double)total) * 100;
		double w_percent = ((double)withdrawn / (double)total) * 100;
		double d_percent = ((double)denied / (double)total) * 100;
		
		DecimalFormat decimalformat = new DecimalFormat("###.##");
		
		String year_count = certified + "   " + certified_withdrawn + "   " + withdrawn + "   " + denied + "   " + total;
		year_count = year_count + "\n\t " + decimalformat.format(c_percent) + "\t  " + decimalformat.format(cw_percent) + "\t  " + decimalformat.format(w_percent) + "\t  " + decimalformat.format(d_percent);
		
		context.write(new Text(year), new Text(year_count));
	}
}