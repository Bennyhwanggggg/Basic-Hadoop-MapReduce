package comp9313.proj1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Project1 {
	
	public static class TFIDFMapper extends Mapper<Object, Text, StringPair, IntWritable> {
		
		private static final IntWritable one = new IntWritable(1);
		private StringPair pair = new StringPair();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString(), " ");
			List<String> terms = new ArrayList<>();
			
			String docID = itr.nextToken();
			
			while (itr.hasMoreTokens()) {
				terms.add(itr.nextToken().toLowerCase());
			}
			for (int i=0; i<terms.size(); i++){
				pair.set(terms.get(i), docID);
				context.write(pair, one);
			}
			
			Set<String> uniqueTerms = new HashSet<String>(terms);
			for (String term: uniqueTerms) {
				pair.set(term, "**");
				context.write(pair, one);
			}
		}
	}
	
	public class PairKeysPartitioner extends Partitioner<StringPair, IntWritable> {

		@Override
		public int getPartition(StringPair key, IntWritable intWritable, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}
	
	public static class TFIDFReducer extends Reducer<StringPair, IntWritable, Text, Text> {
		private DoubleWritable tf = new DoubleWritable();
		private DoubleWritable df = new DoubleWritable();
		private DoubleWritable weight = new DoubleWritable();
		
		public void reduce(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val: values) {
				sum += val.get();
			}
			
			if (key.getSecond().equals("**")) {
				df.set(sum);
			} else {
				tf.set(sum);
				double idf = Math.log10(4/df.get());
				weight = new DoubleWritable(tf.get() * idf);
				Text term = new Text(key.getFirst());
				Text result = new Text(key.getSecond() + ", " + weight.toString());
				context.write(term, result);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		
		// Get input and output path
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		// Create config and setup folders and files
		Configuration conf = new Configuration();
		
		// Start term frequency first
		Job job = Job.getInstance(conf, "Get_TF_DF");
		job.setJarByClass(Project1.class);
		job.setMapperClass(TFIDFMapper.class);
		job.setReducerClass(TFIDFReducer.class);
//		job.setNumReduceTasks(3);
		job.setPartitionerClass(PairKeysPartitioner.class);
		job.setMapOutputKeyClass(StringPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
