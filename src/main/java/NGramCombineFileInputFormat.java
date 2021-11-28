/********************************************
 *File: NGramCombineFileInputFormat.java
 *Usage: Driver
 ********************************************/
import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapred.JobClient;
        import org.apache.hadoop.mapred.JobConf;
        import org.apache.hadoop.mapred.RunningJob;
        import org.apache.hadoop.mapred.TextOutputFormat;
        import org.apache.hadoop.util.GenericOptionsParser;

public class NGramCombineFileInputFormat {

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf("NGramCombineFileInputFormat");
        conf.set("mapred.max.split.size", "134217728");//128 MB
        conf.setJarByClass(NGramCombineFileInputFormat.class);
        String[] jobArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();

        conf.setMapperClass(NGramMapper.class);
        conf.setInputFormat(ExtendedCombineFileInputFormat.class);
        ExtendedCombineFileInputFormat.addInputPath(conf, new Path(jobArgs[0]));

        conf.setNumReduceTasks(0);

        conf.setOutputFormat(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(conf, new Path(jobArgs[1]));
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            Thread.sleep(1000);
        }

        System.exit(job.isSuccessful() ? 0 : 2);
    }
}