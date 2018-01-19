
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.Text;


public class Driver6 
{
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		
		if(args.length == 3)
		{
			conf.set("yearFilter", args[2]);
		}	
		else
		{
			System.out.println("Usage: GPDriver <input dir> <output dir> <year>");
			System.exit(-1);
		}
		
//		MapReduce Job - 1
		Job job1 = Job.getInstance(conf, "Case status wise application count for each year");
		job1.setJarByClass(Driver6.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		Path outputPath1 = new Path("/niit_final_project/query_6/job1_output");
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, outputPath1);
		
		FileSystem.get(conf).delete(outputPath1, true);
		
		job1.waitForCompletion(true);
		
		
//		MapReduce Job - 2
		Job job2 = Job.getInstance(conf, "Case status wise percentage for each year");
		job2.setJarByClass(Driver6.class);
		job2.setMapperClass(MapperPercent.class);
		job2.setReducerClass(ReducerPercent.class);
//		job2.setNumReduceTasks(0);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job2, outputPath1);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		System.exit(job2.waitForCompletion(true)?0:1);
	}
}
