package src;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class NumberMovieByReleaseYear {
	
	public static class Map extends Mapper<LongWritable,Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text year = new Text();
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] splittedLine = line.split("\\t| ");
			year.set(splittedLine[splittedLine.length-1]);
			context.write(year, one);
		}
	}
	public static class Reduce extends Reducer<Text, IntWritable,Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable value : values){
			 sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	

	public static void main(String[] args) throws Exception{
		Job job = new Job();
		job.setJarByClass(NumberMovieByReleaseYear.class);
		job.setJobName("NumberMovieByReleaseYear");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
