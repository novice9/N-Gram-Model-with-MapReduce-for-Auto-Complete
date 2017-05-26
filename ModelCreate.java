import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class ModelCreate {
	
	public static class ModelCreateMapper 
			extends Mapper<LongWritable, Text, Text, Text> {
		
		private int threshold;
		
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			threshold = conf.getInt("threshold", 20);
		}
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			if (value == null) {
				return;
			}
			
			String line = value.toString().trim();
			if (line.length() == 0) {
				return;
			}
			
			String [] tuple = line.split("\t");
			if (tuple.length < 2) {
				return;
			}
			
			String [] words = tuple[0].split("\\s+");
			int freq = Integer.valueOf(tuple[1]);
			
			if (freq < threshold || words.length < 2) {
				return;
			}
			
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < words.length - 1; ++i) {
				sb.append(words[i]).append(" ");
			}
			String follow = words[words.length - 1];
			String prefix = sb.toString().trim();
			
			context.write(new Text(prefix), new Text(follow + "=" + tuple[1]));
		}
	}
	
	public static class ModelCreateReducer
	extends Reducer<Text, Text, DBIntf, NullWritable> {
		
		private int topK;
		
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", 10);
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			PriorityQueue<Entry> entryQueue = new PriorityQueue<Entry>();
			for (Text value : values) {
				String [] tuple = value.toString().trim().split("=");
				entryQueue.add(new Entry(Integer.valueOf(tuple[1]), tuple[0]));
				while (entryQueue.size() > topK) {
					entryQueue.poll();
				}
			}
			
			while (!entryQueue.isEmpty()) {
				Entry cur = entryQueue.poll();
				context.write(new DBIntf(key.toString(), cur.theVal, cur.theKey), NullWritable.get());
			}
		}
	}
}
