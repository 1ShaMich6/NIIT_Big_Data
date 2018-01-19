package query_5;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable.DecreasingComparator;


public class TopProfitProductsDriver 
{
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		
		//Map-Reduce 1
		Job job1 = Job.getInstance(conf, "Products which earn profit Computation");
		job1.setJarByClass(TopProfitProductsDriver.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
		job1.setNumReduceTasks(1);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);
		
		Path outputPath1 = new Path("/retail/job1_output9");
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, outputPath1);
		
		FileSystem.get(conf).delete(outputPath1, true);
		
		job1.waitForCompletion(true);
		
		
		//Map-Reduce 2
		Job job2 = Job.getInstance(conf, "Top 5 viable products for each age group Computation");
		job2.setJarByClass(TopProfitProductsDriver.class);
		job2.setMapperClass(SortMapper.class);
		
		job2.setPartitionerClass(SortPartitioner.class);
		
	//	job2.setSortComparatorClass(DecreasingComparator.class);
		job2.setReducerClass(SortReducer.class);
		job2.setNumReduceTasks(11);
//		job2.setNumReduceTasks(0);
		
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job2, outputPath1);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
