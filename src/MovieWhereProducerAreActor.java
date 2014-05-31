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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * We use this class for create a list of movie where a actor is also a producer
 * @author clément demonchy
 *
 */
public class MovieWhereProducerAreActor {
	private static final String SEPARATOR = "\t";

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text actor = new Text();
		private Text filenameAndTitle = new Text();
		
		/**
		 * @input 'pseudo' Nom, Prenom (num)	"Titre du film" (annee) {titre episode (#s.e)} (as alias) [role] <ordre>
		 * or
		 * @input 'pseudo' Nom, Prenom (num)	"Titre du film" (annee) {titre episode (#s.e)} (role_producer) (as alias)
		 * in both case we can extract the name and the title with the same mapping
		 * @output actor_name	filename/
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String fileName = ((FileSplit) context.getInputSplit()).getPath()
					.getName();
			String line = value.toString();
			String[] splitByTab = line.split(SEPARATOR);
			actor.set(splitByTab[0]);
			try {
				filenameAndTitle.set(fileName + SEPARATOR
						+ splitByTab[1].split("\\(")[0]);
				context.write(actor, filenameAndTitle);
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> moviePlayByActor = new ArrayList<String>();
			ArrayList<String> movieProduceByActor = new ArrayList<String>();
			String[] filenameAndTitle;
			String currentMovie;

			//for every actor we split his movie in two list, producers and actor
			for (Text value : values) {
				filenameAndTitle = value.toString().split(SEPARATOR);
				if (filenameAndTitle.length == 2) {
					if (filenameAndTitle[0]
							.equalsIgnoreCase("producers.list.tsv")) {
						movieProduceByActor.add(filenameAndTitle[1]);
					} else {
						moviePlayByActor.add(filenameAndTitle[1]);
					}
				}
			}
			// if we match movie title en both list we write it and remove the movie from both list, else we only remove it from producer list
			// upgrade available, use sort arrayList, so we can continue as soon we get
			// over in alphanumeric order
			while (movieProduceByActor.size() > 0) {
				currentMovie = movieProduceByActor.get(0);
				int numberMoviePlayByActor = moviePlayByActor.size();
				for (int i = 0; i < numberMoviePlayByActor; i++) {
					if (currentMovie.equals(moviePlayByActor.get(i))) {
						context.write(key, new Text(currentMovie));
						moviePlayByActor.remove(i);
						break;
					}
				}
				movieProduceByActor.remove(0);
			}
			moviePlayByActor.clear();
		}
	}

	public static void main(String[] args) throws Exception {
		@SuppressWarnings("deprecation")
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
