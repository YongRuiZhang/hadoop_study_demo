package com.zhang.mapreduce.patitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean outValue = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
            throws IOException, InterruptedException {

        // 1. 遍历 values,将其中的上行流量,下行流量分别累加
        // phoneNum - [bean1,bean2]
        Long sumUp = 0L;
        Long sumDown = 0L;
        for (FlowBean value : values) {
            sumUp += value.getUpFlow();
            sumDown += value.getDownFlow();
        }

        // 2. 封装 outKV
        outValue.setUpFlow(sumUp);
        outValue.setDownFlow(sumDown);
        outValue.setSumFlow();

        // 3. 写出
        context.write(key, outValue);
    }
}
