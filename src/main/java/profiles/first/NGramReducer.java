package profiles.first;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import utils.OutputLimiter;

/**
 * ============== PROFILE ONE REDUCER ================
 * Outputs the first 500 unigrams of the corpus,
 * sorted alphabetically.
 */
public class NGramReducer extends Reducer<Text, NullWritable, Text, NullWritable> implements
    OutputLimiter {

  private int counter = 0;

  @Override
  protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
    if (isWithinLimit()) {
      context.write(key, NullWritable.get());
      counter++;
    }
  }

  @Override
  public boolean isWithinLimit() {
    return this.counter < OUTPUT_LIMIT;
  }
}