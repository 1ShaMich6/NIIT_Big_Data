
import java.io.BufferedReader;
//import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class POSMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	private Map<String, String> lookUp = new HashMap<String, String>();
	
	protected void setup(Context context) throws IOException, InterruptedException
	{
		super.setup(context);
		
		URI[] file = context.getCacheFiles();
		
		Path p = new Path(file[0]);
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		
		if(p.getName().equals("store_master"))
		{
//			BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));
			String line = null;
			
			while((line = reader.readLine()) != null)
			{
				String[] tokens = line.split(",");
				String store_id = tokens[0];
				String state = tokens[2];
				
				lookUp.put(store_id, state);
			}
			
			reader.close();
		}

		if(lookUp.isEmpty())
		{
			throw new IOException("MyError: Unable to load LookUp Data");
		}
	}
	
	
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String[] record = value.toString().split(",");
		
		String store_id = record[0];
		String prod_id = record[1];
		String qty = record[2];
		
		String state = lookUp.get(store_id);
		String val = qty + "," + state;
		
		context.write(new Text(prod_id), new Text(val));
	}
}
