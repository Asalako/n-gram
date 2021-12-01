import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Input path gs://coc105-gutenburg-10000books/
 * Output path gs://dataproc-buck1/output
 * Output2 path gs://dataproc-buck1/output2 example where I make use of the partitioner to separate each letter to a
 * separate reducer
 */

public class NGram {
    public static class MapperTask extends Mapper<LongWritable, Text, Text, IntWritable>{

        /**
         * Mapper which splits word into tokens, removing all punctuation and digits
         */
        private final Text word = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase().replaceAll("[^a-zA-Z ]", ""));
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                ctx.write(word, one);
            }
        }
    }

    public static class ReducerTask
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * Reducer which sums up the occurrences with in the map
         */
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class PartitionerTask extends Partitioner<Text, IntWritable> {


        /**
         * Split the mapper's output by the numeric value of the first character of the word.
         */

        @Override
        public int getPartition(Text key, IntWritable value, int i) {
            String word = key.toString();
            int firstChar = word.charAt(0);
            return ((firstChar-1) % i);
        }
    }


    public static void main(String[] args) throws Exception {

        /**
         * Configuration for the job and the runner for the hadoop task
         */
        Configuration conf = new Configuration();

        //*********************************
        // max size of each split is set
        conf.set("mapreduce.input.fileinputformat.split.maxsize", Long.toString(128 * 1024 * 1024));
        Job job = Job.getInstance(conf, "Single N Gram");

        job.setJarByClass(NGram.class);
        job.setMapperClass(MapperTask.class);
        job.setReducerClass(ReducerTask.class);
        job.setCombinerClass(ReducerTask.class);
        job.setPartitionerClass(PartitionerTask.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //*********************************
        // Adding the inputs CombineTextInputFormat for less input splits
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //job.setNumReduceTasks(26); //set to 26 to separate each letter
        job.setNumReduceTasks(1); //set to one reducer for a single file
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
