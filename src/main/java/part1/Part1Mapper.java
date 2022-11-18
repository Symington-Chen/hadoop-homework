package part1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Part1Mapper extends Mapper<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void map(Text key, LongWritable value, Mapper<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
