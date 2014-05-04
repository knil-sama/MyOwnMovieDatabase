package src;
/**

 * dntt@u-cergy.fr 2014/03/05 : Template simple pour utilisation de MapReduce pour Hadoop 2.x
 */
import java.io.IOException ;
import java.lang.InterruptedException ;
import org.apache.hadoop.fs.Path ;
import org.apache.hadoop.conf.Configuration ;

import org.apache.hadoop.mapreduce.Mapper ; 
import org.apache.hadoop.mapreduce.Reducer ;
import org.apache.hadoop.mapreduce.Job ;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat ;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat ;

/* pour les import des types TYPECLEF1, TYPECLEF2, TYPECLEF3, TYPEVAL1, TYPEVAL2 et TYPEVAL3, 
   a moins de definir vous meme vos types en implementant  WritableComparable, les types predefinis 
   IntWritable,  LongWritable, Text, etc.  sont tous dans org.apache.hadoop.io. */

public class Template {
}

/*  static class TemplateMapper extends Mapper <TYPECLEF1, TYPEVAL1, TYPECLEF2, TYPEVAL2> {
    public void map (TYPECLEF1 clef, TYPEVAL1 valeur, Context contexte) 
      throws IOException, InterruptedException {
        // traiter valeur (la ligne du fichier lue) pour en extraire ce qu'il faudra envoyer aux reducer
        // la clef (par defaut l'offset de la ligne en cours sur le fichier est rarement utilise.
        contexte.write (new TYPECLEF2 (...), new TYPEVAL2 (...)) ;
  }

  public static class TemplateReducer extends Reducer <TYPECLEF2, TYPEVAL2, TYPECLEF3, TYPEVAL3> {
    public void reduce (TYPECLEF2 clef, Iterable <TYPEVAL2> valeurs, 
      Context contexte) throws IOException, InterruptedException {
          // pour parcourir l'espace des valeurs associees a la clef traitee par ce reducer
          for (TYPEVAL2 val : valeurs) {
              // traitement de val.get () le plus souvent une agregation qui sera utilisee apres
          }
          contexte.write (new TYPECLEF3 (...), new TYPEVAL3 (...)) ;
    }
  }

  public static void main (String args []) throws Exception {
    if (args.length != 2) {
      System.err.println (args.length + "("+args [0] + "," +args [1] + ")") ;
      System.err.println ("Usage : Template <source> <destination>") ;
      System.exit (-1) ;
    }

    Job job = new Job () ;
    job.setJarByClass (Template.class) ;

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

    job.setMapperClass (TemplateMapper.class) ;
    //job.setCombinerClass (TemplateCombiner.class) ;
    job.setReducerClass (TemplateReducer.class) ;

    job.setMapOutputKeyClass (TYPECLEF2.class) ;
    job.setMapOutputValueClass (TYPEVAL2.class) ;

    job.setOutputKeyClass (TYPECLEF3.class) ;
    job.setOutputValueClass (TYPEVAL3.class) ;

    // On peut changer le nombre de reducer. A 0, seul map est utilise.
    // Ne pas mettre cette ligne et laisser par defaut est generalement satisfaisant
    //job.setNumReduceTasks (2) ;

    System.exit (job.waitForCompletion (true) ? 0 : 1) ;
  }
}
*/