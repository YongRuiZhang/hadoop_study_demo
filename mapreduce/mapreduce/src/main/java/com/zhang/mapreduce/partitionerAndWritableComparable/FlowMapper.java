package com.zhang.mapreduce.partitionerAndWritableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    private FlowBean outKey = new FlowBean();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
            throws IOException, InterruptedException {
        // 获取一行 13315688577	4481	22681	27162
        String line = value.toString();
        // 切割 [13315688577,4481,22681,27162]
        String[] split = line.split("\t");
        // 封装
        outKey.setUpFlow(Long.parseLong(split[1]));
        outKey.setDownFlow(Long.parseLong(split[2]));
        outKey.setSumFlow();
        outValue.set(split[0]);
        // 写出
        context.write(outKey, outValue);
    }
}
