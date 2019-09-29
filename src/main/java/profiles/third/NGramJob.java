package profiles.third;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ================== PROFILE ONE DRIVER ======================
 * This is the main class. Hadoop will invoke the main method of this class.
 *
 * Emit unigrams for all the given Wikipedia articles, sorted alphabetically, without duplicates.
 * Punctuation is removed, case is ignored, stop-words are considered as well as gender.
 *
 * Only emits the first 500 unigrams to final output.
 *
 * :::::Configuration:::::::
 * Only uses 1 reducer, since the shuffle phase sorts by key, and we want the reducer to have the first
 * 500 alphabetically sorted words. No combiner is used.
 */
public class NGramJob {
  public static void main(String[] args) {
    try {
      Configuration conf = new Configuration();
      // Give the MapRed job a name. You'll see this name in the Yarn webapp.
      Job job = Job.getInstance(conf, "Profile_1");
      // Current class.
      job.setJarByClass(NGramJob.class);
      // Mapper
      job.setMapperClass(NGramMapper.class);
      // Reducer
      job.setReducerClass(NGramReducer.class);
      // Outputs from the Mapper.
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);

      // Use 1 reducer
      //job.setNumReduceTasks(1);
      // Outputs from Reducer. It is sufficient to set only the following two properties
      // if the Mapper and Reducer has same key and value types. It is set separately for
      // elaboration.
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      // path to input in HDFS
      FileInputFormat.addInputPath(job, new Path(args[0]));
      // path to output in HDFS
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      // Block until the job is completed.
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    } catch (IOException e) {
      System.err.println(e.getMessage());
    } catch (InterruptedException e) {
      System.err.println(e.getMessage());
    } catch (ClassNotFoundException e) {
      System.err.println(e.getMessage());
    }

  }


}
