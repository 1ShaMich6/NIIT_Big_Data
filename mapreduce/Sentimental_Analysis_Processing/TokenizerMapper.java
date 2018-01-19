import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.net.URI;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;


public class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private Map<String, String> Dictionary = new HashMap<String, String>();
	
	private final static IntWritable total_value = new IntWritable();
	private Text word = new Text();
	
	String myWord = "";
	int myValue = 0;
	
	
	protected void setup(Context context) 
			throws IOException, InterruptedException
	{
		super.setup(context);
		
		URI[] files = context.getCacheFiles(); //getCacheFiles() returns null
		
		Path p = new Path(files[0]);
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		
		
		if(p.getName().equals("AFINN.txt"))
		{
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));
			String line = null;
			
			while((line = reader.readLine()) != null)
			{
				String[] tokens = line.split("\t");
				String diction_word = tokens[0];
				String diction_value = tokens[1];
				
				Dictionary.put(diction_word, diction_value);
			}
			
			reader.close();
			
			if(Dictionary.isEmpty())
			{
				throw new IOException("ERROR: Unable to load Dictionary data");
			}
		}
	}
	
	
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		StringTokenizer itr = new StringTokenizer(value.toString());
		
		while(itr.hasMoreTokens())
		{
			myWord = itr.nextToken().toLowerCase();
			
			//Basically if the word is present in the sentiment dictionary
			if(Dictionary.get(myWord) != null)
			{
				myValue = Integer.parseInt(Dictionary.get(myWord));
				
				if(myValue > 0) //Positive sentiment value
				{
					myWord = "positive";
				}
				
				if(myValue < 0) //Negative sentiment value
				{
					myWord = "negative";
					myValue = myValue * -1;  //Convert myValue to its absolute value 
					//REASON : The '-' sign has already been incorporated into the Sentiment Percent Formula
				}

			}
			
			else
			{
				myWord =  "positive";
				myValue = 0;
			}
			
			word.set(myWord);
			total_value.set(myValue);
			
			context.write(word, total_value);
		}
	}
}
