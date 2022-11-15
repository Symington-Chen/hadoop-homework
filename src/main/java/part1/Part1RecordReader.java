package part1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class Part1RecordReader extends RecordReader<Text, LongWritable> {
    private Configuration configuration;
    private FileSplit split;

    private boolean isProgress= true;
    private LongWritable value = new LongWritable(1);
    private Text k = new Text();

    public Part1RecordReader() {
        super();
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit)split;
        configuration = context.getConfiguration();

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(isProgress){
            // 获取文件路径
            String path = split.getPath().toString();
            // 获取文件类型
            String fileclass = path.split("/")[-2];
            // 设置输输出的key
            k.set(fileclass);
        }
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public LongWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
