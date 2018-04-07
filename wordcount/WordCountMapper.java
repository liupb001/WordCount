package wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*input file context:
 * 
 * hello a
 * hello b
 * hello c
 * 
 * */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		
		String line = value.toString();  // get each line, eg: "hello a"
		String[] words = line.split(" ");  // split line to get each word, eg:{hello, a}
		
		// for loop to get each word count, eg:
		// hello 1
		// a 1
		for (String word : words) {  
			context.write(new Text(word), new IntWritable(1));
		}
	}

}
