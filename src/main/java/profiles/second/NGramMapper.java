package profiles.second;

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
 * ================= PROFILE TWO MAPPER =======================
 * Mapper: Reads line by line, split them into words. Emits <documentID, word> pairs.
 */
public class NGramMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

  // A Map of documentIDs to articles, each of which is a Map containing unigrams and their frequencies.
  Map<Integer, Map<String, Unigram>> articles = new HashMap<>();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line         = value.toString();
    Integer documentId  = 0; // Default doc ID if it's missing
    String articleTitle = ""; // Default title if it's missing

    /*
     * Either the first line or the last seems to be empty,
     * so we end up skipping that line if it does not contain "<====>"
     */
    if (line.contains("<====>")) {
      // Handle title and document ID
      String[] splitString = line.split("<====>");
      articleTitle  = splitString[0];
      documentId    = Integer.parseInt(splitString[1]);
      line          = splitString[2];

      // Create data structure to store frequencies of all the words in this article
      Map<String, Unigram> unigrams = new HashMap<>();

      // Tokenize into words delimited by whitespace
      StringTokenizer tokenizer = new StringTokenizer(line);

      // Emit <documentID, word> pairs.
      while (tokenizer.hasMoreTokens()) {
        // Grab a token, remove all punctuation, and lower-case it
        String token = tokenizer.nextToken().trim().replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
        if (!token.isEmpty()) {
          // Add the token to the frequencies map if absent, otherwise increment the token's count
          if (!unigrams.containsKey(token)) {
            unigrams.put(token, new Unigram(token));
          }
          else {
            unigrams.get(token).incrementFrequency();
          }
        }
      }

      // We should now have a completed map of unigram frequencies for a given article.
      // Iterate through and write their values/frequencies to context with the documentID as the key.
      for (String token: unigrams.keySet()) {
        context.write(
            new IntWritable(documentId),
            new Text(unigrams.get(token).toString())
        );
      }
    }
  }
}
