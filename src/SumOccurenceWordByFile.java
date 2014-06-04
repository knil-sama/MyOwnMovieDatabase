package src;

import java.io.IOException;
import java.lang.InterruptedException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
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
		Text outputKeyMap = new Text();
		Text outputValueMap = new Text();
		/**
		 *  @input filename/word	numberOccurence
		 *  @output filename	word/numberOccurence 
		 */
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String filenameAndWord = key.toString();
			int indexSeparator = filenameAndWord.indexOf(separator);
            String filename = filenameAndWord.substring(0,indexSeparator);
            String word = filenameAndWord.substring(indexSeparator+1);
            outputKeyMap.set(filename);
            outputValueMap.set(word + separator + value.toString());
			context.write(outputKeyMap,outputValueMap);
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		Iterator<String> iter;
		Iterator<Text> iteratorValue;
		ArrayList<String> wordsAndOccurences;
		Text value = new Text();
		Text outputKeyReduce = new Text();
		Text outputValueReduce = new Text();
		/**
		* @input filename	word/numberOccurence
		* @output filename/word	numberOccurence/numberOccurenceForFile
		*/
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int numberWordInFile = 0;
			wordsAndOccurences = new ArrayList<String>();
			String wordAndOccurence;
			String numberOccurence;
			int indexOfSeparator;
			iter = wordsAndOccurences.iterator();
			String filename = key.toString();
			String word;
			iteratorValue = values.iterator();
			//we compute the total of word in a file
			while(iteratorValue.hasNext()){
				value.set(iteratorValue.next());
				wordAndOccurence = value.toString();
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
				outputKeyReduce.set(filename + separator + word);
				outputValueReduce.set(numberOccurence + separator + String.valueOf(numberWordInFile));
				context.write(outputKeyReduce,outputValueReduce);
			}
		}
	}

	public static void main(String args[]) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage : Template <source> <destination>");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		conf.set("mapreduce.map.output.compress","true");
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
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