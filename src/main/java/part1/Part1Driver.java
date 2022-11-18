package part1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Part1Driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2 设置jar包路径
        job.setJarByClass(Part1Driver.class);
        //3 关联mapper和reducer
        job.setMapperClass(Part1Mapper.class);
        job.setReducerClass(Part1Reducer.class);
        //4 设置自定义inputFormat
        job.setInputFormatClass(Part1InputFormat.class);
        //5 设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //6 设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //7 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\Code\\hadoop_homework\\data\\part1in\\SPAIN"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\Code\\hadoop_homework\\data\\part1out"));
        //8 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
