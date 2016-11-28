package mapred.hashtagsim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
	
		String line = value.toString();
		
		
		String[] valueSplit = line.split("\t");
		
		String hashtag = valueSplit[0];
		
		Map<String, Integer> features_Vector = parseFeatureVector(valueSplit[1]);
	
		for (Map.Entry <String,Integer> entry1 : features_Vector.entrySet()){
			String entry1_key = entry1.getKey();
			Integer entry1_val = entry1.getValue();
			for (Map.Entry <String,Integer> entry2 : features_Vector.entrySet()){
				String entry2_key = entry2.getKey();
			    Integer entry2_val = entry2.getValue();
			    //if(entry1_key != entry2_key && entry1_key.compareTo(entry2_key)>=0){
			    if(entry1_key.compareTo(entry2_key)>0){
			    	context.write(new Text(entry1_key+" "+entry2_key),
						new IntWritable(entry1_val*entry2_val));
			    }
			}
		}
	}

	/**
	 * This function is ran before the mapper actually starts processing the
	 * records.
	 * 
	 * This function is not used, hence left empty.
	 */
	@Override
	protected void setup(Context context) {

	}

	/**
	 * De-serialize the feature vector into a map
	 * 
	 * @param featureVector
	 *            The format is "word1:count1;word2:count2;...;wordN:countN;"
	 * @return A HashMap, with key being each word and value being the count.
	 */
	private Map<String, Integer> parseFeatureVector(String featureVector) {
		Map<String, Integer> featureMap = new HashMap<String, Integer>();
		String[] features = featureVector.split(";");
		for (String feature : features) {
			String[] word_count = feature.split(":");
			featureMap.put(word_count[0], Integer.parseInt(word_count[1]));
		}
		return featureMap;
	}
}














