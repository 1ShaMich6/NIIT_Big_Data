
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class Driver1B 
{
	public static void main(String args[]) throws Exception
	{
		if(args.length != 2)
		{
			System.out.println("Usage: GPDriver <input dir> <output dir>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		
//		MapReduce Job - 1
		Job job1 = Job.getInstance(conf, "Year wise application count for each job title");
		job1.setJarByClass(Driver1B.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		Path outputPath1 = new Path("/niit_final_project/query_1b/job1_output");
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, outputPath1);
		
		FileSystem.get(conf).delete(outputPath1, true);
		
		job1.waitForCompletion(true);
		
		
//		MapReduce Job - 2
		Job job2 = Job.getInstance(conf, "Average Growth calculation for each job title");
		job2.setJarByClass(Driver1B.class);
		job2.setMapperClass(AvgMapper.class);
		job2.setSortComparatorClass(DecreasingComparator.class);
		job2.setReducerClass(AvgReducer.class);
//		job2.setNumReduceTasks(0);
		
		job2.setMapOutputKeyClass(DoubleWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job2, outputPath1);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		System.exit(job2.waitForCompletion(true)?0:1);
	}
}
