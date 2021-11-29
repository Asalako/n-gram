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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
public class NGram {
    public class MapperCIPWC extends Mapper<LongWritable, Text, Text, IntWritable>{

        private Text outKey = new Text();
        private IntWritable outValue = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split("\t");
            for(String s:words){
                outKey.set(s);
                ctx.write(outKey,outValue);
            }
        }
    }

    public class ReducerCIPWC extends Reducer<Text, IntWritable, Text, IntWritable>{

        private Text outKey = new Text();
        private IntWritable outValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable v: values){
                sum += v.get();
            }
            outKey.set(key);
            outValue.set(sum);
            ctx.write(outKey, outValue);
        }

    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        //*********************************
        // It is important to set the maximum size of each combined split (in bytes)
        // In this case, I'm using the hadoop's block size to keep it similar than the
        // job with only one big file using the PartitionerWordCount job
        conf.set("mapreduce.input.fileinputformat.split.maxsize", Long.toString(128 * 1024 * 1024));

        Job job = Job.getInstance(conf, "Combined Input & Partitioner Word Count");
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
        job.setNumReduceTasks(4);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
