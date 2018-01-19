import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class SPDriver 
{
	public static void main(String args[]) throws Exception
	{
		if(args.length != 3)
		{
			System.out.println("ERROR: 3 arguments required!");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
//		conf.set("mapreduce.output.textoutputformat.separator", ",");
		
		Job job = Job.getInstance(conf);
		job.setJobName("Total Purchase and Sales Quantity");
		job.setJarByClass(SPDriver.class);
		job.setReducerClass(ReduceJoinReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SalesMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PurchaseMapper.class);
		
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
		
		Path outputPath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
