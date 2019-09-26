package profiles.second;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class NGramReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

  int counter = 0;

  @Override
  protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
    if (this.counter++ < 500) {
      context.write(key, NullWritable.get());
    }
  }
}
