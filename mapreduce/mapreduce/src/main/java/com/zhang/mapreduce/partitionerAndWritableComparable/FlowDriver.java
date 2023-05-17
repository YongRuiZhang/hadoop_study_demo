package com.zhang.mapreduce.partitionerAndWritableComparable;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1. 获取配置信息以及获取 job 对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //2. 关联本 Driver 程序的 jar
        job.setJarByClass(FlowDriver.class);

        //3. 关联 Mapper 和 Reducer 的 jar
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4. 设置 Mapper 输出的 kv 类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //5. 设置最终输出 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6. 设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\桌面\\output"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\桌面\\output2"));

        //7. 提交 job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

}
