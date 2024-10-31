package bda_project;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringJoiner;
public class reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // set to store document IDs for each unique word
        Set<String> document_Ids = new HashSet<>();
        // iterate through all document IDs and adding them to the set
        for (Text value : values) {
            document_Ids.add(value.toString());
        }
        // joining the document IDs into a comma-separated string so that it will a list which is value in key value pair
        StringJoiner documentIdsString = new StringJoiner(", ");
        for (String docId : document_Ids) {
            documentIdsString.add(docId);
        }
        // emitting the word as key and the concatenated list of document IDs as value
        context.write(key, new Text(documentIdsString.toString()));
    }
}
