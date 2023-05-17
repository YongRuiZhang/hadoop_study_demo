package com.zhang.mapreduce.etl;

import com.zhang.mapreduce.outputFormat.LogDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WebLogDriver {

    public static void main(String[] args) throws Exception {
// 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[] { "D:/桌面/input", "D:/桌面/output" };
// 1 获取 job 信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
// 2 加载 jar 包
        job.setJarByClass(LogDriver.class);
// 3 关联 map
        job.setMapperClass(WebLogMapper.class);
// 4 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
// 设置 reduceTask 个数为 0
        job.setNumReduceTasks(0);
// 5 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
// 6 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
