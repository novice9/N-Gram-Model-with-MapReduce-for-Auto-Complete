import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class NGramBuild {
	
	public static class NGramBuildMapper 
			extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private int noGram;
		
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			noGram = conf.getInt("noGram", 4);
		}
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Mapper Method
			String rawStr = value.toString().trim().toLowerCase();
			rawStr = rawStr.replaceAll("[^a-z]", " ");
			
			String [] words = rawStr.split("\\s+");
			
			if (words.length < 2) {
				return;
			}
			
			for (int i = 0; i < words.length; ++i) {
				String phrase = words[i];
				for (int j = 1; j < noGram; ++j) {
					if (i + j == words.length) {
						break;
					}
					phrase += (" " + words[i + j]);
					context.write(new Text(phrase.trim()), new IntWritable(1));
				}
			}
		}
	}
	
	public static class NGramBuildReducer
			extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// Reducer method
			int count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			context.write(key, new IntWritable(count));
		}
	}
	
}
