package part1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Part1Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private int sum;
    private LongWritable v = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        // 1 累加求和
        sum = 0;
        for (LongWritable value : values) {
            sum+=value.get();
        }
        System.out.println("$$$$$$$$文件总数$$$$$$$$"+"\n"+sum);
        // 2 输出
        v.set(sum);
        context.write(key, v);
    }
}
