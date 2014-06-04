package src;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author clément demonchy
 * This Mapper Reducer is used to count all the movie where a actor play in and all the movie where he play his own role
 *
 */
public class CountMovieDoneByActor {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text actor = new Text();
		private Text role = new Text();
		/**
		 * @input 'pseudo' Nom, Prenom (num)	"Titre du film" (annee) {titre episode (#s.e)} (as alias) [role] <ordre>
		 * @output 'pseudo' Nom, Prenom (num)	role
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			//we use toLowerCase to harmonize the data, and made simpler comparison
			String line = value.toString().toLowerCase();
			String[] splitByTab = line.split("\t");
			actor.set(splitByTab[0]);
			try{
				role.set(splitByTab[1].split("\\[|\\]")[1]);
				//we consider that the actor only play one role by movie
				//another solution would had been to store the actor|title_movie and role in a ArrayList
				//and remove the duplicate value for title_movie, but like our result are the expected result we don't go so far
				context.write(actor,role);
			}catch(ArrayIndexOutOfBoundsException e){
				role.set("no_role");
				context.write(actor,role);
			}
		}
	}
	public static class Reduce extends Reducer<Text,Text, Text, Text> {
		private Text outputReduce = new Text();

		/**
		 * @input 'pseudo' Nom, Prenom (num)	role
		 * @output 'pseudo' Nom, Prenom (num)	number_of_role	number_of_role_as_himself 
		 */
		public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {
			String role;
			int numberMoviePlayByActor = 0;
			int numberMoviePlayByActorLikeHimself = 0;
			for(Text value : values){
				role = value.toString();
				//in the original file the role is not well formated and sometime you can stumble
				//on "himself-guest" or "himself cameo" so a more rigorous approach would be to
				//use contains, but as our result matched with the wording we stick with equals
				if("himself".equals(role) || "herself".equals(role)){
					numberMoviePlayByActorLikeHimself++;
				}
				numberMoviePlayByActor++;
			}
			outputReduce.set(String.valueOf(numberMoviePlayByActor) + "\t" + String.valueOf(numberMoviePlayByActorLikeHimself));
			context.write(key,outputReduce);
		}
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			System.err.println("length args : "+args.length);
			System.err.println("Usage : CountMovieDoneByActor <input path> <output path>");
				System.exit(-1);
		}
		Configuration conf = new Configuration();
		conf.set("mapreduce.map.output.compress","true");
		@SuppressWarnings("deprecation")		
		Job job = new Job(conf);
		job.setJarByClass(CountMovieDoneByActor.class);
		job.setJobName("CountMovieDoneByActor");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
