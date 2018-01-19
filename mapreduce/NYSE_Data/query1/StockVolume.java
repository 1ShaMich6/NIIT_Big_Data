package query1;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class StockVolume 
{
	public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException
		{
			String[] record = value.toString().split(",");
			String stockName = record[1];
			long volume = Long.parseLong(record[7]);
			
			context.write(new Text(stockName), new LongWritable(volume));
			
		}
	}
	
	
	public static class ReduceClass extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		public void reduce(Text key, Iterable<LongWritable> values, Context context) 
				throws IOException, InterruptedException
		{
			long totalVol = 0;
			
			for(LongWritable val:values)
			{
				totalVol += val.get();
			}
			
			context.write(key, new LongWritable(totalVol));
		}
	}
	
	
	public static void main(String args[]) throws Exception
	{
		if(args.length != 2)
		{
			System.out.println("Usage: StockVolume <input dir> <output dir>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Total_Volume_Conputation");
		
		job.setJarByClass(StockVolume.class);
		job.setMapperClass(MapClass.class);
		job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(ReduceClass.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
