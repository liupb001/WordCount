package wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * final output result:
 * 
 *  a	1
 *	b	1
 *	c	1
 *	hello	3
 *
 */

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException{
		
		Integer count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		context.write(key, new IntWritable(count));
	}
	

}
