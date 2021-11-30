/********************************************
 *File: NGramMapper.java
 *Usage: Mapper
 ********************************************/
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class NGramMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, IntWritable> {

    Text txtKey = new Text("");
    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value,
                    OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                txtKey.set(itr.nextToken());
                output.collect(txtKey, one);
        }

    }

}

