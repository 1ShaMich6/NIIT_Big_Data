
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;


public class Driver8 
{
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		
		if(args.length == 4)
		{
			conf.set("fullTimePosition", args[2]);
			conf.set("yearFilter", args[3]);
		}	
		else
		{
			System.out.println("Usage: GPDriver <input dir> <output dir> <full_time_position> <year>");
			System.exit(-1);
		}
		
		
//		MapReduce Job - 1
		Job job1 = Job.getInstance(conf, "Average prevailing wage for each job title");
		job1.setJarByClass(Driver8.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(DoubleWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		
		Path outputPath1 = new Path("/niit_final_project/query_8/job1_output");
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, outputPath1);
		
		FileSystem.get(conf).delete(outputPath1, true);
		
		job1.waitForCompletion(true);
		
		
//		MapReduce Job - 2
		Job job2 = Job.getInstance(conf, "Descending sort based on average prevailing wage");
		job2.setJarByClass(Driver8.class);
		job2.setMapperClass(SortMapper.class);
		job2.setSortComparatorClass(DecreasingComparator.class);
		job2.setReducerClass(SortReducer.class);
		
		job2.setMapOutputKeyClass(DoubleWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job2, outputPath1);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		System.exit(job2.waitForCompletion(true)?0:1);
	}
}
