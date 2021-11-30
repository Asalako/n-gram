import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
//import org.apache.hadoop.mapred.lib.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import java.io.IOException;
import java.util.StringTokenizer;

public class NGram {
    public static class MapperCIPWC extends Mapper<LongWritable, Text, Text, IntWritable>{

        private final Text word = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("[^a-zA-Z ]", ""));
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                ctx.write(word, one);
            }
        }
    }

    public static class ReducerCIPWC
            extends Reducer<Text, IntWritable, Text, IntWritable> {
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

    public static class MapTask extends
            Mapper<LongWritable, Text, IntWritable, Text> {
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\t"); // This is the delimiter between Key and Value
            int valuePart = Integer.parseInt(tokens[1]);
            context.write(new IntWritable(valuePart), new Text(tokens[0]));
        }
    }

    public static class ReduceTask extends
            Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<IntWritable> list, Context context)
                throws java.io.IOException, InterruptedException {

            for (IntWritable value : list) {

                context.write(value, new Text(String.valueOf(key)));

            }

        }
    }

    public static class PartitionerCIWC extends Partitioner<Text, IntWritable> {


        /**
         *
         * @param key
         * @param value
         * @param i
         * @return
         *
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
        Configuration conf = new Configuration();

        //*********************************
        // It is important to set the maximum size of each combined split (in bytes)
        // In this case, I'm using the hadoop's block size to keep it similar than the
        // job with only one big file using the PartitionerWordCount job
        conf.set("mapreduce.input.fileinputformat.split.maxsize", Long.toString(128 * 1024 * 1024));

        Job job = Job.getInstance(conf, "Single N Gram");
        job.setJarByClass(NGram.class);
        job.setMapperClass(MapperCIPWC.class);
        job.setReducerClass(ReducerCIPWC.class);

        job.setCombinerClass(ReducerCIPWC.class);
        job.setPartitionerClass(PartitionerCIWC.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //*********************************
        // Adding the inputs CombineTextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf, "Sort by Value");

        job2.setJarByClass(NGram.class);
        job2.setMapperClass(MapTask.class);
        job2.setReducerClass(ReduceTask.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

