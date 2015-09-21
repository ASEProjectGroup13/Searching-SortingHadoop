

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortingMapperClass extends Mapper<Object, Text, Text, IntWritable>{
	
	private Text keysData = new Text();
	private IntWritable valesData = new IntWritable();
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		StringTokenizer strTokenize = new StringTokenizer(value.toString());
		
		while (strTokenize.hasMoreElements()) {
			 
			int number = Integer.parseInt(strTokenize.nextToken());
			
			keysData.set(number + "");
			
			valesData.set(number);
			
			context.write(keysData, valesData);
			
		}
	}
	
	

}
