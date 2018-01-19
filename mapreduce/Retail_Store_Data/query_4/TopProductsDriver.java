package query_4;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;


public class TopProductsDriver 
{
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		
		//Map-Reduce 1
		Job job1 = Job.getInstance(conf, "Finding the total transaction amount for each product");
		job1.setJarByClass(TopProductsDriver.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(DoubleWritable.class);
		job1.setOutputKeyClass(DoubleWritable.class);
		job1.setOutputValueClass(Text.class);
		
		Path outputPath1 = new Path("/retail/intermediate_output");
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, outputPath1);
		
		FileSystem.get(conf).delete(outputPath1, true);
		job1.waitForCompletion(true);
		
		
//		Map-Reduce 2
		Job job2 = Job.getInstance(conf, "Sorting the data based on total transaction amount");
		job2.setJarByClass(TopProductsDriver.class);
		job2.setMapperClass(SortMapper.class);
		job2.setReducerClass(SortReducer.class);
		
		job2.setSortComparatorClass(DecreasingComparator.class);
		
		job2.setMapOutputKeyClass(DoubleWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job2, outputPath1);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
