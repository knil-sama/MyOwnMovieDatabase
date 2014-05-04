package src;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CountMovieDoneByActor {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text actor = new Text();
		private Text role = new Text();

		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] splitByTab = line.split("\t");
			actor.set(splitByTab[0]);
			try{
				role.set(splitByTab[1].split("\\[|\\]")[1]);
				//we consider that the actor only play one role by movie
				context.write(actor,role);
			}catch(ArrayIndexOutOfBoundsException e){
				e.printStackTrace();
			}
		}
	}
	public static class Reduce extends Reducer<Text,Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {
			int numberMoviePlayByActor = 0;
			int numberMoviePlayByActorLikeHimself = 0;
			for(Text value : values){
				if("himself".equalsIgnoreCase(value.toString())){
					numberMoviePlayByActorLikeHimself++;
				}
				numberMoviePlayByActor++;
			}
			context.write(key, new Text(String.valueOf(numberMoviePlayByActor) + " " + String.valueOf(numberMoviePlayByActorLikeHimself)));
		}
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			System.err.println("length args : "+args.length);
			System.err.println("Usage : CountMovieDoneByActor <input path> <output path>");
				System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(CountMovieDoneByActor.class);
		job.setJobName("CountMovieDoneByActor");
		

		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		
		job.setReducerClass(Reduce.class);

		///* can be done manually
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
