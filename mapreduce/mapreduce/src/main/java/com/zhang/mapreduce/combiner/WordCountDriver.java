package com.zhang.mapreduce.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

//        1. 获取配置信息以及获取 job 对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

//        2. 关联本 Driver 程序的 jar
        job.setJarByClass(WordCountDriver.class);

//        3. 关联 Mapper 和 Reducer 的 jar
        job.setMapperClass(WordCountMapper.class); // 关联 Mapper
        job.setReducerClass(WordCountReducer.class); // 关联 Reducer

//        4. 设置 Mapper 输出的 kv 类型 ,注意有Map，没有map的是第5步调用的方法
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

//        5. 设置最终输出 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setCombinerClass(WordCountCombiner.class);

//        6. 设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\桌面\\hello.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\桌面\\hello_output"));

//        7. 提交 job
        boolean result = job.waitForCompletion(false);// 参数true表示需要获取job的工作信息，false不用

        // 根据结果返回值，如果result == true（成功）返回0，如果失败返回1
        System.exit(result ? 0 : 1);

    }

}
