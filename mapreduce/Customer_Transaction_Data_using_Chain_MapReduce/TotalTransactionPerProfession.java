import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;


public class TotalTransactionPerProfession 
{
	public static void main(String args[]) 
			throws Exception
	{
		Configuration conf = new Configuration();
		
		//Map-Reduce 1
		Job job1 = Job.getInstance(conf, "<Profession> - <Total Transaction Amount per Customer ID>");
		job1.setJarByClass(TotalTransactionPerProfession.class);
		job1.setReducerClass(Reducer1.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		
		MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, CustMapper.class);
		MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, StoreMapper.class);
		
//		Path outputPath1 = new Path("FirstMapper");   ---> will create the output in /user/hduser folder
		Path outputPath1 = new Path("/customer_chain_mapreduce/FirstReducer1");
		FileOutputFormat.setOutputPath(job1, outputPath1);

		FileSystem.get(conf).delete(outputPath1, true);
		
		job1.waitForCompletion(true);
		
		
		//Map-Reduce 2
		Job job2 = Job.getInstance(conf, "<Total Transaction Amount per Profession> - <Profession>");
		job2.setJarByClass(TotalTransactionPerProfession.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(DoubleWritable.class);
		job2.setOutputKeyClass(DoubleWritable.class);
		job2.setOutputValueClass(Text.class);
		
		Path outputPath2 = new Path("/customer_chain_mapreduce/SecondReducer1");
		
		FileInputFormat.addInputPath(job2, outputPath1);
		FileOutputFormat.setOutputPath(job2, outputPath2);
		
		FileSystem.get(conf).delete(outputPath2, true);
		
		job2.waitForCompletion(true);
		
		
		//Map-Reduce 3
		Job job3 = Job.getInstance(conf, "<Profession> - <Total Transaction Amount per Profession>  ---> In Descending order");
		job3.setJarByClass(TotalTransactionPerProfession.class);
		job3.setMapperClass(SortMapper.class);
		job3.setReducerClass(SortReducer.class);
		
		//VERY IMPORTANT - If you want sorting in the DESCENDING order
		job3.setSortComparatorClass(DecreasingComparator.class);
		
		job3.setMapOutputKeyClass(DoubleWritable.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job3, outputPath2);
		FileOutputFormat.setOutputPath(job3, new Path(args[2]));
		
		FileSystem.get(conf).delete(new Path(args[2]), true);
		
		System.exit(job3.waitForCompletion(true) ? 0 : 1);
	}
}
