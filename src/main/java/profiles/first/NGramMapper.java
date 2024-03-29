package profiles.first;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


/**
 * ============== PROFILE ONE MAPPER ====================
 * Mapper: Reads line by line, split them into words. Emits <word, NullWritable> pairs.
 */
public class NGramMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

  private Set<String> unigrams = new HashSet<>();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
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
        if (!token.isEmpty() && !unigrams.contains(token)) {
          unigrams.add(token);
          context.write(new Text(token), NullWritable.get());
        }
      }
    }


  }
}
