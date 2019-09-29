package profiles.third;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * ================ PROFILE ONE COMBINER ====================
 * Combiner: Input to the combiner is the output from the mapper.
 * The combiner is not used in this profile.
 */
public class NGramCombiner extends Reducer<Text, NullWritable, Text, NullWritable> {
  @Override
  protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
    context.write(key, NullWritable.get());
  }
}
