package src;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author clément demonchy this Mapper Reducer count the number of repetition
 *         for a movie/serie title for imb file, movies.list.tsv
 * 
 */
public class NumberOccurenceTitle {
	// input "title_movie" (année_tournage) titre_episode (#s.e) annee_sortie
	// ouput "title_movie" 1
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		
		private final IntWritable one = new IntWritable(1);
		private Text title = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// we use lower case to not miss title on upper/lower case
			// difference
			String line = value.toString().toLowerCase();
			title.set(line.split("\\(")[0]);
			context.write(title, one);
		}
	}

	// "title_movie" 1
	// "title_movie" number_of_same_title_movie
	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> iterator = values.iterator();
			@SuppressWarnings("unused")
			IntWritable value;
			while(iterator.hasNext()){
				value = iterator.next();
				// sum += value.get();
				// in this case we already know that the value is 1 so to
				// optimize we don't try to get the real value
				sum += 1;
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage : Template <source> <destination>");
			System.exit(-1);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job();

		job.setJarByClass(NumberOccurenceTitle.class);
		job.setJobName("NumberOccurenceTitle");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
