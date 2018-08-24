package comp9313.proj1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Project1 {
	
//	static enum NumDoc {
//		COUNT;
//	}
//	
//	// Mapper for document counting job..
//	public static class DocCountMapper extends Mapper<Object, Text, StringPair, IntWritable> {
//		
//		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
//			
//			// Simply update counter to count number of lines which is the number of documents.
//			context.getCounter(NumDoc.COUNT).increment(1);
//		}
//	}
	
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
			
			// Get all the unique term count and use * to ensure it gets sorted to the top and processed by reducer first 
			Set<String> uniqueTerms = new HashSet<String>(terms);
			for (String term: uniqueTerms) {
				pair.set(term, "*");
				context.write(pair, one);
			}
//			
//			// Update the number of document, N, count
//			context.getCounter(NumDoc.COUNT).increment(1);
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
	
	public static class PairKeysPartitioner extends Partitioner<StringPair, IntWritable> {

		@Override
		public int getPartition(StringPair key, IntWritable intWritable, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}
	
	public static class TFIDFReducer extends Reducer<StringPair, IntWritable, Text, Text> {

		private DoubleWritable df = new DoubleWritable();

		public void reduce(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			// Get N
			String nDoc = context.getConfiguration().get("NumDoc");
			double n = Double.parseDouble(nDoc);
			
			int sum = 0;
			for (IntWritable val: values) {
				sum += val.get();
			}
					
			// Calculate the document frequency of the given term
			if (key.getSecond().equals("*")) {
				df.set(sum);
			} else {
				// Calculate tf.idf using the formula
				double tf = sum;
				double idf = Math.log10(n/df.get());
				DoubleWritable weight = new DoubleWritable(tf * idf);
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
		
		FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
		FileStatus[] status = hdfs.listStatus(inputPath);
		double nLines = 0;
		for (FileStatus f: status) {
			if (f.isFile()) {
				FSDataInputStream inputStream = hdfs.open(f.getPath());
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
				
				String line = reader.readLine();
				
				while (line != null) {
					nLines ++;
					line = reader.readLine();
				}
				
				if (reader != null) {
					reader.close();
				}
			}
		}
//		System.out.println(nLines);
//
//		// Find total number of document first
//		Job countJob = Job.getInstance(conf, "DocCount");
//		countJob.setJarByClass(Project1.class);
//		countJob.setMapperClass(DocCountMapper.class);
//		countJob.setNumReduceTasks(0);	// no reduce job needed for as we are only interested in the counter we get from config
//		FileInputFormat.addInputPath(countJob, inputPath);
//		FileOutputFormat.setOutputPath(countJob, outputPath);
//		countJob.waitForCompletion(true);
//		
//		long n = countJob.getCounters().findCounter(NumDoc.COUNT).getValue(); // Extract N and pass it to the next job
//		
//		// Delete output dir
//		if (hdfs.exists(outputPath)) {
//			hdfs.delete(outputPath, true);
//		}
//
//		// Start tf-idf calculation 
//		Configuration tfidfConf = new Configuration();
//		tfidfConf.set("TotalDocCount", Long.toString(n));
		conf.set("NumDoc", Double.toString(nLines));
		Job job = Job.getInstance(conf, "Get_TF_DF");
		job.setJarByClass(Project1.class);
		job.setMapperClass(TFIDFMapper.class);
		job.setReducerClass(TFIDFReducer.class);
		job.setPartitionerClass(PairKeysPartitioner.class);
		job.setMapOutputKeyClass(StringPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setGroupingComparatorClass(PairKeysGroupingComparator.class);
		job.setNumReduceTasks(3);
	
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
