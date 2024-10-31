package bda_project;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;
public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        // ensuring each text file is not split and processing as a whole by a single mapper
        return false;
    }
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // creating a new record reader
        WholeFileRecordReader reader = new WholeFileRecordReader();
        // initializing the record reader with the input split and context
        reader.initialize(split, context);
        return reader;
    }
    public static class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {
        private FileSplit fileSplit;
        private JobContext jobContext;
        private Text key;
        private BytesWritable value;
        private boolean processed = false;
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            // casting and storing the split as a FileSplit
            this.fileSplit = (FileSplit) split;
            // storing the job context
            this.jobContext = context;
        }
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!processed) {
                // buffer to store file contents
                byte[] contents = new byte[(int) fileSplit.getLength()];
                // getting the path of the file to be read
                Path file = fileSplit.getPath();
                // getting the FileSystem object
                FileSystem fs = file.getFileSystem(jobContext.getConfiguration());
                try (FSDataInputStream in = fs.open(file)) {
                    // reading the entire file into the buffer
                    in.readFully(contents);
                }
                // setting the key as the file name and value as the file contents and marking this file as processed
                key = new Text(file.getName());
                value = new BytesWritable(contents);
                processed = true;
                return true;
            }
            return false;
        }
        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            // returning the current key (file name)
            return key;
        }
        @Override
        public BytesWritable getCurrentValue() throws IOException, InterruptedException {
            // returning the current value (file contents)
            return value;
        }
        @Override
        public float getProgress() throws IOException {
            // returning the progress of the record reader
            return processed ? 1.0f : 0.0f;
        }
        @Override
        public void close() throws IOException {
        }
    }
}