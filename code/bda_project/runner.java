package bda_project;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class runner {
    public static void main(String[] args) throws Exception {
        // creating a new object for hadoop configuration
        Configuration conf = new Configuration();
        // setting job name
        Job job = Job.getInstance(conf, "Inverted Index using Hadoop MapReduce");
        // setting the jar file by finding the runner class
        job.setJarByClass(runner.class);
        // setting the custom input format class to mapper
        job.setInputFormatClass(WholeFileInputFormat.class);
        // setting mapper, reducer classes
        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);
        // setting the output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // setting the paths for input and output directories
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // exiting the program if job is success
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}