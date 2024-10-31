package bda_project;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.StringTokenizer;
public class mapper extends Mapper<Text, BytesWritable, Text, Text> {
    // text object to store unique word
    private Text word = new Text();
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        // hash set to store unique words in current document
        HashSet<String> unique_words = new HashSet<>();
        // converting document to string
        String content = new String(value.getBytes(), 0, value.getLength(), StandardCharsets.UTF_8);
        StringTokenizer tokenizer = new StringTokenizer(content);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            // text processing - converting to lowercase and removing punctuation
            token = token.toLowerCase().replaceAll("[^a-zA-Z]", "");

            if (!token.isEmpty() && unique_words.add(token)) {
                // processing each word i.e setting the word in Text object
                word.set(token);
                // emitting the word and its document ID
                context.write(word, key);
            }
        }
    }
}