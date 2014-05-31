package src;

import java.io.IOException;
import java.lang.InterruptedException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CalculTD_IDF {
	
	public static String SEPARATOR = new String("/");
	public static float NUMBER_FILE_INPUT = 3;
	public static int NUMBER_PLACE_PODIUM = 10;

	public static class IDF_Filename {
		public Double td_idf;
		public String filename;

		public IDF_Filename(Double td_idf_input, String filename_input) {
			td_idf = td_idf_input;
			filename = filename_input;
		}
	}
	
	public static void insertionSort(IDF_Filename[] sortedTable, IDF_Filename  element_to_insert){
		IDF_Filename output_tmp;
		for (int index = 0; index < NUMBER_PLACE_PODIUM; index++) {
			if (sortedTable[index] == null) {
				sortedTable[index] = element_to_insert;
				break;
			} else if (sortedTable[index].td_idf < element_to_insert.td_idf) {
				output_tmp = sortedTable[index];
				sortedTable[index] = element_to_insert;
				element_to_insert = output_tmp;
			}
		}
	}

	public static class Map extends Mapper<Text, Text, DoubleWritable, Text> {
		IDF_Filename[] podium = new IDF_Filename[NUMBER_PLACE_PODIUM];
		/**
		 * every time we find the word we seek, we try to insert it in a podium sorted by desc order and with
		 * limited place
		 */
		public void map(Text key, Text valeur, Context context)
				throws IOException, InterruptedException {
			String valeurString = valeur.toString();
			String[] tmpValeurs = valeurString.split(SEPARATOR);
			Double numberOccurenceByWord = Double.valueOf(tmpValeurs[0]);
			Double sumOccurenceByFile = Double.valueOf(tmpValeurs[1]);
			Double wordFrequency = Double.valueOf(tmpValeurs[2]);
			String[] tmpKey = key.toString().split(SEPARATOR);
			String filename = tmpKey[0];
			String word = tmpKey[1];
			String wordResearch = context.getConfiguration()
					.get("wordResearch");
			Double td_idf = 0.0;
			if (word.equals(wordResearch)) {
				td_idf = (numberOccurenceByWord / sumOccurenceByFile)
						* Math.log(NUMBER_FILE_INPUT / wordFrequency);
				IDF_Filename output_to_insert = new IDF_Filename(td_idf,
						filename);
				// insert sort desc
				insertionSort(podium, output_to_insert);	
			}
		}
/**
 * we write the content of the podium
 * @output td_idf	filename
 */
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			for (IDF_Filename entry : podium) {
				if (entry != null)
					context.write(new DoubleWritable(entry.td_idf), new Text(
							entry.filename));
			}
		}

		public static void main(String args[]) throws Exception {
			if (args.length != 3) {
				System.err
						.println("Usage : Template <source> <destination> <research word");
				System.exit(-1);
			}
			// we use conf to send the word that the user input
			Configuration conf = new Configuration();
			conf.set("wordResearch", args[2].toLowerCase());
			@SuppressWarnings("deprecation")
			Job job = new Job(conf);

			job.setJarByClass(CalculTD_IDF.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.setInputFormatClass(KeyValueTextInputFormat.class);

			job.setMapperClass(Map.class);

			job.setMapOutputKeyClass(DoubleWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(DoubleWritable.class);
			job.setOutputValueClass(Text.class);

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
}
