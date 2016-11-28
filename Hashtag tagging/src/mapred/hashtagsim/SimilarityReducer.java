package mapred.hashtagsim;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarityReducer extends Reducer<Text, IntWritable,  Text, IntWritable> {


	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
		Context context)
		throws IOException, InterruptedException {		
			
		int count = 0;

		for (IntWritable x : value) {
	
			count=count+x.get();
		}	
			
		/*
		 * We're serializing the word cooccurrence count as a string of the following form:
		 * 
		 * word1:count1;word2:count2;...;wordN:countN;
		 */
		context.write(key, new IntWritable(count));
	}
}
