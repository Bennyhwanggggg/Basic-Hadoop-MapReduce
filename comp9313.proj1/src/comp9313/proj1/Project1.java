package comp9313.proj1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
	
	public static class TFMapper extends Mapper<Object, Text, StringPair, IntWritable> {
		
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
	
	public static class TFReducer extends Reducer<StringPair, IntWritable, StringPair, IntWritable> {
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

	public static void main(String[] args) throws Exception {
		
		// Get input and output path
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		// Create config and setup folders and files
		Configuration conf = new Configuration();
		
		// Start term frequency first
		Job job1 = Job.getInstance(conf, "Get_TF_DF");
		job1.setJarByClass(Project1.class);
		job1.setMapperClass(TFMapper.class);
		job1.setReducerClass(TFReducer.class);
		job1.setPartitionerClass(PairKeysPartitioner.class);
		job1.setMapOutputKeyClass(StringPair.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		job1.setNumReduceTasks(3);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
}
