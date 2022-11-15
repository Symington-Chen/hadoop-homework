package part1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Part1Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    int sum;
    LongWritable v = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        // 1 累加求和
        sum = 0;
        for (LongWritable value : values) {
            sum+=value.get();
        }
        // 2 输出
        v.set(sum);
        context.write(key, v);
    }
}
