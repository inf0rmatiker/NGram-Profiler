package profiles.second;

import java.io.IOException;
import java.util.PriorityQueue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.OutputLimiter;
import utils.Unigram;


/**
 * ============== PROFILE TWO REDUCER =====================
 * Outputs the first 500 unigrams for each article,
 * sorted by occurrence frequency.
 */
public class NGramReducer extends Reducer<IntWritable, Text, IntWritable, Text> implements
    OutputLimiter {

  private int currentOutput = 0;

  @Override
  protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    PriorityQueue<Unigram> sortedUnigrams = new PriorityQueue<>();

    for (Text value: values) {
      sortedUnigrams.add(new Unigram(value));
    }

    while (!sortedUnigrams.isEmpty() && isWithinLimit()) {
      context.write(key, sortedUnigrams.poll().getText());
      currentOutput++;
    }

  }

  @Override
  public boolean isWithinLimit() {
    return currentOutput < OUTPUT_LIMIT;
  }
}