package profiles.second;

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
 * ================ PROFILE TWO DRIVER ========================
 * This is the main class. Hadoop will invoke the main method of this class.
 *
 * :::::::Configuration:::::::::
 * Currently not using combiner for testing...
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
      // Combiner. We use the reducer as the combiner in this case.
      //job.setCombinerClass(NGramCombiner.class);
      // Reducer
      job.setReducerClass(NGramReducer.class);
      // Outputs from the Mapper.

      // Use 5 reducer
      job.setNumReduceTasks(5);

      job.setMapOutputKeyClass(IntWritable.class); // <key = docID, ...>
      job.setMapOutputValueClass(Text.class); // <..., value = word>
      // Outputs from Reducer. It is sufficient to set only the following two properties
      // if the Mapper and Reducer has same key and value types. It is set separately for
      // elaboration.
      job.setOutputKeyClass(IntWritable.class); // <key = docID, ...>
      job.setOutputValueClass(Text.class); // <..., value = unigram[SPACE]frequency>
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
