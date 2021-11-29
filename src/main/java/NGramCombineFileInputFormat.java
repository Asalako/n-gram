/********************************************
 *File: NGramCombineFileInputFormat.java
 *Usage: Driver
 ********************************************/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NGramCombineFileInputFormat {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NGramCombineFileInputFormat");
        conf.set("mapred.max.split.size", "134217728");//128 MB
        job.setJarByClass(NGramCombineFileInputFormat.class);
        String[] jobArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();

        job.setMapperClass(NGramMapper.class);
        conf.setInputFormat(ExtendedCombineFileInputFormat.class);
        ExtendedCombineFileInputFormat.addInputPath(conf, new Path(jobArgs[0]));

        conf.setNumReduceTasks(1);

        conf.setOutputFormat(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(conf, new Path(jobArgs[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            Thread.sleep(1000);
        }

        System.exit(job.isSuccessful() ? 0 : 2);
    }
}