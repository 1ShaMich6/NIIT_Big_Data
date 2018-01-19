
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TSDriver 
{
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", "|");
		
		if(args.length > 2)
		{
			conf.set("myText", args[2]);
		}
		else
		{
			System.out.println("The number of arguments should be 3.");
			System.exit(0);
		}
		
		Job job = Job.getInstance(conf, "String search");
		job.setJarByClass(TSDriver.class);
		job.setMapperClass(TSMapper.class);
		job.setReducerClass(TSReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
