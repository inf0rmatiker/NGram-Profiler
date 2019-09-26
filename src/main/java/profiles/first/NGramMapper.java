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
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class NGramMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

  Set<String> unigrams = new HashSet<>();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();

    // Handle title and document ID
    if (line.contains("<====>")) {
      String[] splitString = line.split("<====>");
      String title      = splitString[0];
      String documentId = splitString[1];
      line       = splitString[2];
      //context.write(new Text(text), new IntWritable(1));
    }

    // Tokenize into words delimited by whitespace
    StringTokenizer tokenizer = new StringTokenizer(value.toString());

    // Emit word, count pairs.
    while (tokenizer.hasMoreTokens()) {
      // Grab a word, remove all punctuation, and lower-case it
      String token = tokenizer.nextToken().trim().replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
      if (!unigrams.contains(token)) {
        unigrams.add(token);
        context.write(new Text(token), NullWritable.get());
      }
      ///context.write(new Text(tokenizer.nextToken()), new IntWritable(1));
    }
  }
}
