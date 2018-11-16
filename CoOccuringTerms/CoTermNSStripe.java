import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import comp9313.lab3.CoTermNSPair.NSPairReducer;
import comp9313.lab3.CoTermNSPair.TokenizerMapper;

import org.apache.hadoop.mapreduce.Reducer;

public class CoTermNSStripe {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable> {
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
			List<String> terms = new ArrayList<>();
			while (itr.hasMoreTokens()) {
				terms.add(itr.nextToken().toLowerCase());
			}
			for (int i=0; i<terms.size(); i++){
				MapWritable record = new MapWritable();
				Text word1 = new Text(terms.get(i));
				for (int j=i+1; j<terms.size(); j++) {
					Text word2 = new Text(terms.get(j));
					
					if (record.containsKey(word2)) {
						IntWritable count = (IntWritable) record.get(word2);
						count.set(count.get() + 1);
						record.put(word2, count);
					} else {
						record.put(word2, new IntWritable(1));
					}
				}
				context.write(word1, record);
			}
		}
	}
	
	public static class NSStripeCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
		
		public void combine(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			
			MapWritable map = new MapWritable();
			
			for (MapWritable val: values) {
				Set<Entry<Writable, Writable>> sets = val.entrySet();
				for (Entry<Writable, Writable> entry: sets) {
					Text word2 = (Text) entry.getKey();
					int count = ((IntWritable)entry.getValue()).get();
					if (map.containsKey(word2)) {
						map.put(word2, new IntWritable((((IntWritable)map.get(word2)).get()) + count));
					} else {
						map.put(word2, new IntWritable(count));
					}
				}
			}
			context.write(key, map);
		}
	}
	
	public static class NSStripeReducer extends Reducer<Text, MapWritable, Text, IntWritable> {
		
		private Text word = new Text();
		private IntWritable count = new IntWritable();
		
		public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			
			HashMap<String, Integer> map = new HashMap<>();
			
			for (MapWritable val: values) {
				Set<Entry<Writable, Writable>> sets = val.entrySet();
				for (Entry<Writable, Writable> entry: sets) {
					String word2 = entry.getKey().toString();
					int count = ((IntWritable)entry.getValue()).get();
					if (map.containsKey(word2)) {
						map.put(word2, map.get(word2) + count);
					} else {
						map.put(word2, count);
					}
				}
			}
			
			// Do sorting to make the result the same as pair approach
			// Use Object[] to hold different types
			Object[] sortKey = map.keySet().toArray();
			Arrays.sort(sortKey);
			for (int i=0; i<sortKey.length; i++) {
				word.set(key.toString() + " " + sortKey[i].toString());
				count.set(map.get(sortKey[i]));
				context.write(word, count);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "term co-occurrence nonsymmetric stripe");
		job.setJarByClass(CoTermNSStripe.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(NSStripeCombiner.class);
		job.setReducerClass(NSStripeReducer.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
