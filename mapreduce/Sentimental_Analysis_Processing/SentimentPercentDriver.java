
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

public class SentimentPercentDriver 
{
	public static void main(String args[]) throws Exception
	{
		if(args.length != 3)
		{
			System.out.println("USAGE: SentimentPercentDriver <dictionary dir> <input dir> <output dir>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Sentiment Percent Computation");
		job.setJarByClass(SentimentPercentDriver.class);
		job.setMapperClass(TokenizerMapper.class);
		job.addCacheFile(new Path(args[0]).toUri());
		job.setReducerClass(IntSumReducer.class);
//		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		System.exit(job.waitForCompletion(true) ? 0 :1);
	}
}
