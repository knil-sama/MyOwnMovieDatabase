package src;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MovieWhereProducerAreActor {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text actor = new Text();
		private Text title = new Text();

		
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] splitByTab = line.split("\t");
			actor.set(splitByTab[0]);
			try{
			title.set(splitByTab[1].split("\\(")[1]);
			context.write(actor,title);
			}catch(ArrayIndexOutOfBoundsException e){
				e.printStackTrace();
			}
		}
	}
	public static class Reduce extends Reducer<Text,Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			ArrayList<String> moviePlayOrProduceByActor = new ArrayList<String>();
			for(Text value : values){
				moviePlayOrProduceByActor.add(value.toString());
			}
			String movie;
			while(moviePlayOrProduceByActor.size() > 1){
				movie = moviePlayOrProduceByActor.get(0);
				int numberMovieLeft = moviePlayOrProduceByActor.size();
				for(int i = 0; i < numberMovieLeft; i ++){
					//if actor is seen more than two then it mean he play both role, but don't work if actor has more than one role in the movie, evolve with using poducer|movie and actor|movie like value
					if(movie.equals(moviePlayOrProduceByActor.get(i))){
						context.write(key, new Text(movie));
					}
				}
				moviePlayOrProduceByActor.remove(0);
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		Job job = new Job();
		job.setJarByClass(MovieWhereProducerAreActor.class);
		job.setJobName("MovieWhereProducerAreActor");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
			
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
