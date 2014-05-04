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

public class TopTen {
	static class TemplateMapper extends Mapper<Text, Text, Text, Text> {
		TreeMap<Double, Text> podium = new TreeMap<Double, Text>();

		public void map(Text clef, Text valeur, Context context)
				throws IOException, InterruptedException {

			podium.put(Double.valueOf(clef.toString()), valeur);
			if (podium.size() > 10) {
				podium.remove(podium.firstKey());
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			Map<Double, Text> iterator = podium.descendingMap();
			for (Entry<Double, Text> entry : iterator.entrySet()) {
				Double cle = entry.getKey();
				Text valeur = entry.getValue();
				context.write(new Text(String.valueOf(cle)), valeur);
			}
		}

		public static void main(String args[]) throws Exception {
			if (args.length != 2) {
				System.err.println(args.length + "(" + args[0] + "," + args[1]
						+ ")");
				System.err.println("Usage : Template <source> <destination>");
				System.exit(-1);
			}

			Job job = new Job();
			job.setJarByClass(Template.class);

			// Le fichier HDFS a utiliser en entree
			FileInputFormat.addInputPath(job, new Path(args[0]));

			// La sortie sera mis sur HDFS sous forme de *repertoire*. Il y a
			// dans ce repertoire un fichier
			// par reducer sous la forme repertoire/part-r-NUMREDUCER. Au cas ou
			// le nombre de reducer est
			// force a 0, seul le resultat de map sera ecrit et sous la forme
			// repertoire/part-m-NUMMAPPER
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			// Par defaut InputFormat utilise par Job est TextInputFormat qui
			// etend
			// FileInputFormat<LongWritable, Text> parsant ligne par ligne le
			// fichier et renvoyant
			// l'offset comme clef et la chaine representant la ligne
			// commevaleur. Vous pouvez changer
			// ce comportement en implementant vos propres InputFormat and
			// RecordReader puis en specifiant
			job.setInputFormatClass(KeyValueTextInputFormat.class);

			job.setMapperClass(TemplateMapper.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// On peut changer le nombre de reducer. A 0, seul map est utilise.
			// Ne pas mettre cette ligne et laisser par defaut est generalement
			// satisfaisant
			// job.setNumReduceTasks (2) ;

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
}
