import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class NSRelativeFreq2 {
	public static class TokenizerMapper extends Mapper<Object, Text, StringPair, IntWritable> {
		
		private static final IntWritable one = new IntWritable(1);
		private StringPair wordPair = new StringPair();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
			List<String> terms = new ArrayList<>();
			while (itr.hasMoreTokens()) {
				terms.add(itr.nextToken().toLowerCase());
			}
			for (int i=0; i<terms.size(); i++){
				for (int j=i+1; j<terms.size(); j++) {
					wordPair.set(terms.get(i), terms.get(j));
					context.write(wordPair, one);
					
					// emit special key
					wordPair.set(terms.get(i), "*");
					context.write(wordPair, one);
				}
			}
		}
	}
	
	public static class NSRelativeFreqCombiner extends Reducer<StringPair, IntWritable, StringPair, IntWritable> {
		
		private IntWritable result = new IntWritable();
		
		public void reduce(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val: values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static class PairKeysGroupingComparator extends WritableComparator {
		
		protected PairKeysGroupingComparator(){
			super(StringPair.class, true);
		}
		
		public int compare(WritableComparable wc1, WritableComparable wc2) {
			StringPair pair1 = (StringPair) wc1;
			StringPair pair2 = (StringPair) wc2;
			int cmp = pair1.getFirst().compareTo(pair2.getFirst());
			if (cmp != 0) {
				return cmp;
			} else {
				return pair1.getSecond().compareTo(pair2.getSecond()); // ensures all [term, *] pair gets send to reducer together because of how StringPair class was modified to sort the integer
			}
		}
	}
	

	
	public static class NSRelativeFreqReducer extends Reducer<StringPair, IntWritable, StringPair, DoubleWritable> {
		
		private DoubleWritable result = new DoubleWritable();
		private double curMarginal;
		
		public void reduce(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val: values) {
				sum += val.get();
			}
			
			if (!key.getSecond().equals("*")) {
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
	public static class RelFreqPartitioner extends Partitioner<StringPair, IntWritable> {
		public int getPartition(StringPair key, IntWritable value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "nonsymmetric relative frequency v2");
		job.setJarByClass(NSRelativeFreq2.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(NSRelativeFreqCombiner.class);
		job.setReducerClass(NSRelativeFreqReducer.class);
		job.setPartitionerClass(RelFreqPartitioner.class);
		job.setMapOutputKeyClass(StringPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(StringPair.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
