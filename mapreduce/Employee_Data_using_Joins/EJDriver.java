
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;


public class EJDriver 
{
	public static void main(String args[]) 
			throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf  = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		
		Job job = Job.getInstance(conf);
		job.setJobName("Map Side Join");
		job.setJarByClass(EJDriver.class);
		job.setMapperClass(EJMapper.class);
		
		job.addCacheFile(new Path(args[1]).toUri());
		job.addCacheFile(new Path(args[2]).toUri());
		
//		job.setReducerClass(EJReducer.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		
		job.waitForCompletion(true);
	}
}