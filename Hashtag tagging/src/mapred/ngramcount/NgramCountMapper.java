

package mapred.ngramcount;

import java.io.IOException;

import mapred.util.Tokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//
import mapred.job.Optimizedjob;

public class NgramCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		//System.out.println(line);
		String[] words = Tokenizer.tokenize(line);
		String nn = context.getConfiguration().get("n");
		int n = Integer.parseInt(nn);
		
		
		int length = words.length; //finding the length of the words array which contains all the tokens.
					

		for (int i=0;i<=words.length-n;i=i+1){
			String[] ngram = new String[n];
			int c;

			StringBuilder value1 = new StringBuilder();
			for (c=0;c<n;c++){
				int k = i+c;
				value1.append(words[k]+" ");				
			}
						
			String aaa = value1.toString();
			context.write(new Text(aaa), NullWritable.get());
		}
	}
}
