package query_6;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;

public class SortPartitioner extends Partitioner<DoubleWritable, Text> 
{
	@Override
	public int getPartition(DoubleWritable key, Text value, int numReduceTasks)
	{
		String[] part = value.toString().split("\\|");
		String ageGroup = part[0];
		
		if(ageGroup.equals("A"))
		{
			return 0 % numReduceTasks;
		}	
		else if(ageGroup.equals("B"))
		{
			return 1 % numReduceTasks;
		}	
		else if(ageGroup.equals("C"))
		{
			return 2 % numReduceTasks;
		}	
		else if(ageGroup.equals("D"))
		{
			return 3 % numReduceTasks;
		}
		else if(ageGroup.equals("E"))
		{
			return 4 % numReduceTasks;
		}
		else if(ageGroup.equals("F"))
		{
			return 5 % numReduceTasks;			
		}	
		else if(ageGroup.equals("G"))
		{
			return 6 % numReduceTasks;
		}
		else if(ageGroup.equals("H"))
		{
			return 7 % numReduceTasks;
		}
		else if(ageGroup.equals("I"))
		{
			return 8 % numReduceTasks;
		}	
		else if(ageGroup.equals("J"))
		{
			return 9 % numReduceTasks;
		}	
		else
		{
			return 10 % numReduceTasks;
		}
	}
}
