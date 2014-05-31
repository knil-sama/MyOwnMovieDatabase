package src;

import java.io.IOException;
import java.lang.InterruptedException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 
 * @author clément demonchy
 *
 */
public class SumOccurenceWordByFile {
	static private String separator = new String("/");
	static class Map extends Mapper<Text, Text, Text, Text> {
		/**
		 *  @input filename/word	numberOccurence
		 *  @output filename	word/numberOccurence 
		 */
		public void map(Text clef, Text valeur, Context context)
				throws IOException, InterruptedException {
			String filenameAndWord = clef.toString();
			int indexSeparator = filenameAndWord.indexOf(separator);
            String filename = filenameAndWord.substring(0,indexSeparator);
            String word = filenameAndWord.substring(indexSeparator+1);
			context.write(new Text(filename),   new Text(word + separator + valeur.toString()));
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		/**
		* @input filename	word/numberOccurence
		* @output filename/word	numberOccurence/numberOccurenceForFile
		*/
		public void reduce(Text clef, Iterable<Text> valeurs, Context context)
				throws IOException, InterruptedException {
			int numberWordInFile = 0;
			ArrayList<String> wordsAndOccurences = new ArrayList<String>();
			String wordAndOccurence;
			String numberOccurence;
			int indexOfSeparator;
			Iterator<String> iter = wordsAndOccurences.iterator();
			String filename = clef.toString();
			String word;
			//we calcul the total of word in a file
			for (Text val : valeurs) {
				wordAndOccurence = val.toString();
				indexOfSeparator = wordAndOccurence.lastIndexOf(separator);
				wordsAndOccurences.add(wordAndOccurence.substring(indexOfSeparator+1));
				numberOccurence = wordAndOccurence.substring(indexOfSeparator+1);
				numberWordInFile += Integer.valueOf(numberOccurence);
			}

			while(iter.hasNext()){
				wordAndOccurence = iter.next();
				indexOfSeparator = wordAndOccurence.lastIndexOf(separator);
				word = wordAndOccurence.substring(0,indexOfSeparator);
				numberOccurence = wordAndOccurence.substring(indexOfSeparator+1);
				context.write(new Text(filename + separator + word),new Text(numberOccurence + separator + String.valueOf(numberWordInFile)));
			}
		}
	}

	public static void main(String args[]) throws Exception {

		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(SumOccurenceWordByFile.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}