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
 * Class Mapper Reducer that get the number of repetition for a word in every file
 */
public class FrequencyWordByFile {
	
	public static String separator = new String("/");
	
	static class Map extends Mapper<Text, Text, Text, Text> {
		String filenameAndWord;
		String filename;
		String word;
		Text keyOutputMap = new Text();
		Text valueOutputMap = new Text();
		/**
		 * @input	filename/word	numberOccurence/numberOccurenceForFile
		 * @output 	filename	word/numberOccurence/NumberWordInFile
		 */
		public void map(Text clef, Text valeur, Context context)
				throws IOException, InterruptedException {
			filenameAndWord = clef.toString();
			int indexSeparator = filenameAndWord.indexOf(separator);
            filename = filenameAndWord.substring(0,indexSeparator);
            word = filenameAndWord.substring(indexSeparator+1);
            keyOutputMap.set(word);
            valueOutputMap.set(filename + separator + valeur.toString());
			context.write(keyOutputMap, valueOutputMap);
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		ArrayList<String> filenameAndOccurenceAndSumList;
		String filenameAndOccurenceAndSum;
		String numberOccurence;
		Iterator<String> iter;
		Iterator<Text> iteratorValue;
		String word, filename, sumOccurence;
		Text value = new Text();
		Text outputKeyReduce = new Text();
		Text outputValueReduce = new Text();
		/**
		 * @input word	filename/numberOccurence/numberWordByFile
		 * @output filename/word	numberOccurence/numberOccurenceForFile
		 */
		public void reduce(Text clef, Iterable<Text> valeurs, Context context)
				throws IOException, InterruptedException {
			int frequencyWordInFile = 0;
			filenameAndOccurenceAndSumList = new ArrayList<String>();
			filenameAndOccurenceAndSum = null;
			iter = filenameAndOccurenceAndSumList.iterator();
			iteratorValue = valeurs.iterator();
			word = clef.toString();
			int firstIndex,lastIndex;
			// we made the sum of every occurrence for each file for a word
			while(iteratorValue.hasNext()) {
				value.set(iteratorValue.next());
				filenameAndOccurenceAndSum = value.toString();
				filenameAndOccurenceAndSumList.add(filenameAndOccurenceAndSum);
				numberOccurence = filenameAndOccurenceAndSum.substring(filenameAndOccurenceAndSum.lastIndexOf("/")+1);
				frequencyWordInFile++;
			}
			// for file we write the output with the frequency we compute previously
			while(iter.hasNext()){
				filenameAndOccurenceAndSum = iter.next();
				firstIndex = filenameAndOccurenceAndSum.indexOf(separator);
				lastIndex = filenameAndOccurenceAndSum.lastIndexOf(separator);
				filename = filenameAndOccurenceAndSum.substring(0,firstIndex);
				numberOccurence = filenameAndOccurenceAndSum.substring(firstIndex+1,lastIndex);
				sumOccurence = filenameAndOccurenceAndSum.substring(lastIndex + 1);
				outputKeyReduce.set(filename + separator + word);
				outputValueReduce.set(numberOccurence + separator + sumOccurence + separator +String.valueOf(frequencyWordInFile));
				context.write(outputKeyReduce,outputValueReduce);
			}
		}
	}

	public static void main(String args[]) throws Exception {

		Configuration conf = new Configuration();
		conf.set("mapreduce.map.output.compress","true");
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		job.setJarByClass(FrequencyWordByFile.class);

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