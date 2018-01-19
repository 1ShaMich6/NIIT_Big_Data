import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class POSPartitioner extends Partitioner<Text, Text>
{
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks)
	{
		String[] str = value.toString().split(",");
		String state = str[1];
		
		if(state.equals("MAH"))
			return (0 % numReduceTasks);
		else
			return (1 % numReduceTasks);
	}
}
