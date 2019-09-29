package profiles.third;

import java.io.IOException;
import java.util.PriorityQueue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.OutputLimiter;
import utils.Unigram;

/**
 * ============== PROFILE THREE REDUCER ================
 * Outputs the top 500 unigrams in the corpus, by frequency.
 */
public class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> implements
    OutputLimiter {

  private int counter = 0;

  PriorityQueue<Unigram> sortedUnigrams = new PriorityQueue<>();

  @Override
  protected void reduce(Text key, Iterable<IntWritable> frequencies, Context context) throws IOException, InterruptedException {
    int unigramFrequency = 0;
    String unigramValue = key.toString();

    for (IntWritable frequency: frequencies) {
      unigramFrequency += frequency.get();
    }

    sortedUnigrams.add(new Unigram(unigramValue, unigramFrequency));
  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    for (; isWithinLimit(); this.counter++) { // Funky for loop
      Unigram nextUnigram = sortedUnigrams.poll();
      context.write(new Text(nextUnigram.getValue()), new IntWritable(nextUnigram.getFrequency()));
    }
  }

  @Override
  public boolean isWithinLimit() {
    return this.counter < OUTPUT_LIMIT;
  }
}