

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SearchingMapperClass extends Mapper<Object, Text, Text, Text>{
	
	private Text keysData = new Text();
	private String aaa;
	private static int temp = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		aaa = ((FileSplit) context.getInputSplit()).getPath()
				.toString();
	}
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		temp++;
		while (itr.hasMoreTokens()) {
			keysData.set(aaa);
			context.write(keysData, new Text(temp + " :: " + itr.nextToken()));
		}
	}
	

}
