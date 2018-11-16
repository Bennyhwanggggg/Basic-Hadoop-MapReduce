import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class NSRelativeFreq {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		private static final IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
			List<String> terms = new ArrayList<>();
			while (itr.hasMoreTokens()) {
				terms.add(itr.nextToken().toLowerCase());
			}
			for (int i=0; i<terms.size(); i++){
				for (int j=i+1; j<terms.size(); j++) {
					word.set(terms.get(i) + " " + terms.get(j));
					context.write(word, one);
					
					// emit special key
					word.set(terms.get(i) + " *");
					context.write(word, one);
				}
			}
		}
	}
	
	public static class NSRelativeFreqCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		
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
	

	
	public static class NSRelativeFreqReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		
		private DoubleWritable result = new DoubleWritable();
		private double curMarginal;
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val: values) {
				sum += val.get();
			}
			
			if (!key.toString().contains("*")) {
				double relFreq = sum/curMarginal;
				result.set(relFreq);
				context.write(key, result);
			} else {
				curMarginal = sum;
			}
		}
	}
	
	/*
	 * If more than 1 reducer is used, we need a partitioner to guarantee that all key value pairs relevant
	 * to term 1 are sent to the same reducer
	 */
	public static class RelFreqPartitioner extends Partitioner<Text, IntWritable> {
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			String firstword = key.toString().split(" ")[0];
			return (firstword.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "nonsymmetric relative frequency");
		job.setJarByClass(NSRelativeFreq.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(NSRelativeFreqCombiner.class);
		job.setReducerClass(NSRelativeFreqReducer.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
