package com.zhang.mapreduce.patitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private Text outKey = new Text();
    private FlowBean outValue = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
            throws IOException, InterruptedException {
        // 1. 获取一行信息
        // 7 	13560436666 	120.196.100.99 	1116	954		200
        String line = value.toString();

        // 2. 切割
        // [7,13560436666,120.196.100.99,1116,954,200]
        String[] split = line.split("\t");

        // 3. 提取想要的数据
        // 手机号：13560436666=>split[1],上行流量：1116=>split[len - 3],下行流量：954=>split[len - 2]
        String phoneNum = split[1];
        String up = split[split.length - 3];
        String down = split[split.length - 2];

        // 4. 封装输出Key-Value
        outKey.set(phoneNum);
        outValue.setUpFlow(Long.parseLong(up));
        outValue.setDownFlow(Long.parseLong(down));
        outValue.setSumFlow();

        // 5.写出
        context.write(outKey, outValue);
    }
}
