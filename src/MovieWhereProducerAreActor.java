package src;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
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
 * 
 * @author clément demonchy
 * 
 */
public class MovieWhereProducerAreActor {
	private static final String SEPARATOR = "\t";

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text actor = new Text();
		private Text filenameAndTitle = new Text();

		/**
		 * @input 'pseudo' Nom, Prenom (num) "Titre du film" (annee) {titre
		 *        episode (#s.e)} (as alias) [role] <ordre> or
		 * @input 'pseudo' Nom, Prenom (num) "Titre du film" (annee) {titre
		 *        episode (#s.e)} (role_producer) (as alias) in both case we can
		 *        extract the name and the title with the same mapping
		 * @output actor_name filename/
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String fileName = ((FileSplit) context.getInputSplit()).getPath()
					.getName();
			String line = value.toString().toLowerCase();
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

		ConcurrentHashMap<String, Integer> moviePlayByActor = new ConcurrentHashMap<String, Integer>();
		ConcurrentHashMap<String, Integer> movieProduceByActor = new ConcurrentHashMap<String, Integer>();
		Set<String> moviesProducer;
		Text value = new Text();
		Text outputReduce = new Text();
		String[] filenameAndTitle;
		String currentMovie;
		Iterator<Text> iteratorValue;
		Iterator<String> iteratorProducer;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			iteratorValue = values.iterator();
			// for every actor we split his movie in two list, producers and
			// actor
			// we use HashMap for remove duplicate value for title and to use
			// O(1) complexity in the research step later
			while (iteratorValue.hasNext()) {
				value.set(iteratorValue.next());
				filenameAndTitle = value.toString().split(SEPARATOR);
				try {
					if (filenameAndTitle[0].equals("producers.list.tsv")) {
						movieProduceByActor.put(filenameAndTitle[1], 0);
					} else {
						moviePlayByActor.put(filenameAndTitle[1], 0);
					}
				} catch (ArrayIndexOutOfBoundsException e) {
					e.printStackTrace();
				}
			}
			moviesProducer = movieProduceByActor.keySet();
			iteratorProducer = moviesProducer.iterator();
			while (iteratorProducer.hasNext()) {
				currentMovie = iteratorProducer.next();
				if (moviePlayByActor.containsKey(currentMovie)) {
					outputReduce.set(currentMovie);
					context.write(key, outputReduce);
				}
			}
			moviePlayByActor.clear();
			movieProduceByActor.clear();
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.map.output.compress", "true");
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
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
