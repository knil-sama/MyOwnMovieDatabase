package src;

/**

 * dntt@u-cergy.fr 2014/03/05 : Template simple pour utilisation de MapReduce pour Hadoop 2.x
 */
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* pour les import des types Text, Text, TYPECLEF3, Text, Text et TYPEVAL3, 
 a moins de definir vous meme vos types en implementant  WritableComparable, les types predefinis 
 IntWritable,  LongWritable, Text, etc.  sont tous dans org.apache.hadoop.io. */

public class TopTenFile {
	private static String wordResearch; 
	static class Map extends Mapper<Text, Text, Text, Text> {
		TreeMap<Double, String> podium = new TreeMap<Double, String>();

		public void map(Text clef, Text valeur, Context context)
				throws IOException, InterruptedException {
			String valeurString = valeur.toString();
			if(valeurString.split("/")[1].equals(wordResearch)){
				podium.put(Double.valueOf(clef.toString()), valeurString);
				if (podium.size() > 10) {
					podium.remove(podium.firstKey());
				}
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			Map<Double, Text> iterator = podium.descendingMap();
			for (Entry<Double, String> entry : iterator.entrySet()) {
				Double cle = entry.getKey();
				String valeur = entry.getValue();
				context.write(new Text(String.valueOf(cle)), valeur.split("/")[0]);
			}
		}

		public static void main(String args[]) throws Exception {
			if (args.length != 3) {
				System.err.println(args.length + "(" + args[0] + "," + args[1]
						+ ")");
				System.err.println("Usage : Template <source> <destination> <research word");
				System.exit(-1);
			}
			wordResearch = args[2];
			Job job = new Job();
			job.setJarByClass(TopTenFile.class);

			// Le fichier HDFS a utiliser en entree
			FileInputFormat.addInputPath(job, new Path(args[0]));

			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.setInputFormatClass(KeyValueTextInputFormat.class);

			job.setMapperClass(Map.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
}
