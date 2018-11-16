import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CoTermNSPair {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		// The count is always one in Pairs method as we use reducer to sum it later
		private static final IntWritable count = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
			List<String> terms = new ArrayList<>();
			while (itr.hasMoreTokens()) {
				terms.add(itr.nextToken().toLowerCase());
			}
			System.out.println(terms.get(0));
			for (int i=0; i<terms.size(); i++){
				for (int j=i+1; j<terms.size(); j++) {
					word.set(terms.get(i) + " " + terms.get(j));
					context.write(word, count);
				}
			}
		}
	}
	
	public static class NSPairReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val: values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "term co-occurrence nonsymmetric pair");
		job.setJarByClass(CoTermNSPair.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(NSPairReducer.class);
		job.setReducerClass(NSPairReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
