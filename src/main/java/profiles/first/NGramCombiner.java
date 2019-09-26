package profiles.first;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class NGramCombiner extends Reducer<Text, NullWritable, Text, NullWritable> {
  @Override
  protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
    context.write(key, NullWritable.get());
  }
}
