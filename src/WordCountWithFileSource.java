package src;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountWithFileSource {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text fileAndWord = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			while(tokenizer.hasMoreTokens()){
				fileAndWord.set(fileName + "/" +tokenizer.nextToken());
				context.write(fileAndWord,one);
			}
			
		}
	}
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException, InterrupteException {
			int sum = 0;
			for(IntWritable value : values){
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception{
		Job job = new Job();
		job.setJarByClass(WordCount.class);
		job.setJobName("Word Count With File Source");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
			
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
}
