

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SearchingReducerClass extends Reducer<Text, Text, Text, IntWritable>{

	private static String searchWord = "";
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		
		searchWord = context.getConfiguration().get(SearchingMainClass.INPUT_WORD);
	}
	
	public void reduce(Text keyIn, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text val : values) {
			String text = val.toString();
			if (text.contains(searchWord)) {
				String[] parts = text.split(" :: ");
				context.write(keyIn, new IntWritable(Integer.parseInt(parts[0])));
			}
		}
	}
	
}
