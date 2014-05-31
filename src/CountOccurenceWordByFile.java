package src;

import java.io.IOException;
import java.lang.InterruptedException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 
 * @author clément demonchy
 *This Mapper Reducer is the first part of indexing chain, here we clean the data and write each word we find
 */
public class CountOccurenceWordByFile {
	
	public static String separator = new String("/");

	static class Map extends Mapper<LongWritable, Text, Text, Text> {
		Text one = new Text("1");

		// format input offset line, output filename/word\t1
		public void map(LongWritable clef, Text valeur, Context context)
				throws IOException, InterruptedException {
			//we get the name of the file from the context
			String fileName = ((FileSplit) context.getInputSplit()).getPath()
					.getName();
			String line = valeur.toString().toLowerCase();
			// remove balise html, then ponctuation
			String clearLine = removePonctuation(line.replaceAll("\\<[^>]*>",
					" "));
			StringTokenizer st = new StringTokenizer(clearLine);
			String word;
			//we remove one letter word
			while (st.hasMoreTokens()) {
				word = st.nextToken();
				if (word.length() > 1) {
					context.write(new Text(fileName + separator + word), one);
				}
			}
		}

		public static String removePonctuation(String stringToClear) {
			String stringCleared = stringToClear.replace(".", " ");
			stringCleared = stringCleared.replace(",", " ");
			stringCleared = stringCleared.replace(";", " ");
			stringCleared = stringCleared.replace(":", " ");
			stringCleared = stringCleared.replace("'", " ");
			stringCleared = stringCleared.replace("$", " ");
			stringCleared = stringCleared.replace("!", " ");
			stringCleared = stringCleared.replace("?", " ");
			stringCleared = stringCleared.replace("&", " ");
			stringCleared = stringCleared.replace(")", " ");
			stringCleared = stringCleared.replace("(", " ");
			stringCleared = stringCleared.replace("{", " ");
			stringCleared = stringCleared.replace("}", " ");
			stringCleared = stringCleared.replace("=", " ");
			stringCleared = stringCleared.replace("+", " ");
			stringCleared = stringCleared.replace("%", "");
			stringCleared = stringCleared.replace("é", "e");
			stringCleared = stringCleared.replace("è", "e");
			stringCleared = stringCleared.replace("ù", "u");
			stringCleared = stringCleared.replace("à", "a");
			stringCleared = stringCleared.replace("`", " ");
			stringCleared = stringCleared.replace("\"", " ");

			return stringCleared;
		}

	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		/** @input filename/word\t1, output
		* @output filename/word\tnumberOccurenceWord
		*/
		public void reduce(Text clef, Iterable<Text> valeurs, Context context)
				throws IOException, InterruptedException {
			int numberOccurenceWordInFile = 0;
			for (@SuppressWarnings("unused")
			Text val : valeurs) {
				numberOccurenceWordInFile++;
			}
			context.write(clef,
					new Text(String.valueOf(numberOccurenceWordInFile)));
		}
	}

	public static void main(String args[]) throws Exception {

		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(CountOccurenceWordByFile.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}