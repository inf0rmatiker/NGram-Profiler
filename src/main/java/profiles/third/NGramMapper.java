package profiles.third;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.Unigram;


/**
 * ============== PROFILE THREE MAPPER ====================
 * Mapper: Reads line by line, split them into words.
 */
public class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  private Map<String, Unigram> unigrams = new HashMap<>();

  @Override
  protected void map(LongWritable key, Text value, Context context) {
    String line = value.toString();

    // Handle title and document ID
    if (line.contains("<====>")) {
      String[] splitString = line.split("<====>");
      String title      = splitString[0];
      String documentId = splitString[1];
      line       = splitString[2];

      // Tokenize into words delimited by whitespace
      StringTokenizer tokenizer = new StringTokenizer(line);

      // Emit word, count pairs.
      while (tokenizer.hasMoreTokens()) {
        // Grab a word, remove all punctuation, and lower-case it
        String token = tokenizer.nextToken().trim().replaceAll("[^a-zA-Z0-9]", "").toLowerCase();

        if (!token.isEmpty()) {
          if (unigrams.containsKey(token)) {
            unigrams.get(token).incrementFrequency();
          }
          else {
            unigrams.put(token, new Unigram(token));
          }
        }
      }

    }

  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    for (String key: unigrams.keySet()) {
      Unigram currentUnigram = unigrams.get(key);
      context.write(new Text(key), new IntWritable(currentUnigram.getFrequency()));
    }
  }
}
