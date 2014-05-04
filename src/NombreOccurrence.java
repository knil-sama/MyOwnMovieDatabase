package src;
/**

 * dntt@u-cergy.fr 2014/03/05 : NombreOccurrence simple pour utilisation de MapReduce pour Hadoop 2.x
 */
import java.io.IOException ;
import java.lang.InterruptedException ;
import java.util.StringTokenizer ;

import org.apache.hadoop.fs.Path ;
import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Mapper ; 
import org.apache.hadoop.mapreduce.Reducer ;
import org.apache.hadoop.mapreduce.Job ;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat ;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat ;

/* pour les import des types LongWritable, Text, Text, Text, IntWritable et IntWritable, 
   a moins de definir vous meme vos types en implementant  WritableComparable, les types predefinis 
   IntWritable,  LongWritable, Text, etc.  sont tous dans org.apache.hadoop.io. */

public class NombreOccurrence {
  static class NombreOccurrenceMapper extends Mapper <LongWritable, Text, Text, IntWritable> {
    public void map (LongWritable clef, Text valeur, Context contexte) 
      throws IOException, InterruptedException {
        // traiter valeur (la ligne du fichier lue) pour en extraire ce qu'il faudra envoyer aux reducer
        // la clef (par defaut l'offset de la ligne en cours sur le fichier est rarement utilise.
	String ligne = valeur.toString () ;
	ligne = ligne.toLowerCase();
	StringTokenizer tokenizer = new StringTokenizer (ligne, ",.?'\";:-(){}[]! ") ;
	FileSplit split = (FileSplit) contexte.getInputSplit();
	String nomFichier = split.getPath().toString();

	while (tokenizer.hasMoreTokens ()) {
		String mot = tokenizer.nextToken().toLowerCase();
		mot = mot.trim();
		
		if (mot.isEmpty())
			continue;

        contexte.write (new Text (mot + "|" + nomFichier ), new IntWritable (1)) ;
	}
  }
  }

  public static class NombreOccurrenceReducer extends Reducer <Text, IntWritable, Text, IntWritable> {
    public void reduce (Text clef, Iterable <IntWritable> valeurs, 
      Context contexte) throws IOException, InterruptedException {
	int somme = 0 ;
    
	
	// pour parcourir l'espace des valeurs associees a la clef traitee par ce reducer
          for (IntWritable val : valeurs) {
              // traitement de val.get () le plus souvent une agregation qui sera utilisee apres
		somme += val.get () ;
          }
          contexte.write (clef, new IntWritable (somme)) ;
          
    }
  }

  public static void main (String args []) throws Exception {
    if (args.length != 2) {
      System.err.println (args.length + "("+args [0] + "," +args [1] + ")") ;
      System.err.println ("Usage : NombreOccurrence <source> <destination>") ;
      System.exit (-1) ;
    }

    Job job = new Job () ;
    job.setJarByClass (NombreOccurrence.class) ;

    // Le fichier HDFS a utiliser en entree
    FileInputFormat.addInputPath (job, new Path (args [0])) ;

    // La sortie sera mis sur HDFS sous forme de *repertoire*. Il y a dans ce repertoire un fichier
    // par reducer sous la forme repertoire/part-r-NUMREDUCER. Au cas ou le nombre de reducer est
    // force a 0, seul le resultat de map sera ecrit et sous la forme repertoire/part-m-NUMMAPPER
    FileOutputFormat.setOutputPath (job, new Path (args [1])) ;

    // Par defaut InputFormat utilise par Job est TextInputFormat qui etend 
    // FileInputFormat<LongWritable, Text> parsant ligne par ligne le fichier et renvoyant 
    // l'offset comme clef et la chaine representant la ligne commevaleur.  Vous pouvez changer 
    // ce comportement en implementant vos propres InputFormat and RecordReader puis en specifiant 
    // job.setInputFormat(MonInputFormat.class);

    job.setMapperClass (NombreOccurrenceMapper.class) ;
    //job.setCombinerClass (NombreOccurrenceCombiner.class) ;
    job.setReducerClass (NombreOccurrenceReducer.class) ;

    job.setMapOutputKeyClass (Text.class) ;
    job.setMapOutputValueClass (IntWritable.class) ;

    job.setOutputKeyClass (Text.class) ;
    job.setOutputValueClass (IntWritable.class) ;

    // On peut changer le nombre de reducer. A 0, seul map est utilise.
    // Ne pas mettre cette ligne et laisser par defaut est generalement satisfaisant
    //job.setNumReduceTasks (2) ;

    System.exit (job.waitForCompletion (true) ? 0 : 1) ;
  }
}
