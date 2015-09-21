

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortingReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private List<Integer> listOfNumbers = new ArrayList<Integer>();
	private Text textKey = new Text();
	
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> values,
			Context context)
					throws IOException, InterruptedException {
		
		
		for(IntWritable intvalues : values){
			listOfNumbers.add(intvalues.get());
		}
	}
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
		Collections.sort(listOfNumbers);
		
		for(Integer a: listOfNumbers){
			context.write(new Text(""), new IntWritable(a));
		}
	}

}
