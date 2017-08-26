package task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FbiCasesReducer  extends Reducer<Text, IntWritable, Text, IntWritable>{

	private int sum;
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,Context context)
	throws IOException, InterruptedException{
		sum = 0;
		
		/*
		 * For every occurrence of key, it's values will be summed up
		 * It's values are contained in the iterable (values). 
		 */
		for (IntWritable value : values) {
				sum += value.get();	//Every int value is 1, and will be accumulated by sum
		}
		
		//Outputting the key and it's values' sum
		context.write(key, new IntWritable(sum));
	}

}
