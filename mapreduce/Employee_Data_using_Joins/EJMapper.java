
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.net.URI;
import java.io.BufferedReader;
//import java.io.FileReader;
import java.io.InputStreamReader;


public class EJMapper extends Mapper<LongWritable, Text, Text, Text>
{
	private Map<String, String> abMap = new HashMap<String, String>();
	private Map<String, String> abMap1 = new HashMap<String, String>();
	
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	protected void setup(Context context) throws IOException, InterruptedException
	{
		super.setup(context);
		
		URI[] files = context.getCacheFiles();
		
		Path p = new Path(files[0]);
		Path p1 = new Path(files[1]);
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		
		
		if(p.getName().equals("salary.txt"))
		{
//			BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));
			
			String line = reader.readLine();
			while(line != null)
			{
				String[] tokens = line.split(",");
				String emp_id = tokens[0];
				String emp_sal = tokens[1];
				
				abMap.put(emp_id, emp_sal);
				line = reader.readLine();
			}
			
			reader.close();
		}
		
		
		if(p1.getName().equals("desig.txt"))
		{
//			BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));
			
			String line = null;
			
			while((line = reader.readLine()) != null)
			{
				String[] tokens = line.split(",");
				String emp_id = tokens[0];
				String emp_desig = tokens[1];
				
				abMap1.put(emp_id, emp_desig);
			}
			
			reader.close();
		}
		
		if(abMap.isEmpty())
		{
			throw new IOException("MyError: Unable to load Salary Data.");
		}
		
		if(abMap1.isEmpty())
		{
			throw new IOException("MyError: Unable to load Designation Data");
		}
	}
	
	
	protected void map(LongWritable Key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String row = value.toString();
		String[] tokens = row.split(",");
		
		String emp_id = tokens[0];
		String salary = abMap.get(emp_id);
		String desig = abMap1.get(emp_id);
		
		String sal_desig = salary + ", " + desig;
		
		outputKey.set(row);
		outputValue.set(sal_desig);
		
		context.write(outputKey, outputValue);
	}
}