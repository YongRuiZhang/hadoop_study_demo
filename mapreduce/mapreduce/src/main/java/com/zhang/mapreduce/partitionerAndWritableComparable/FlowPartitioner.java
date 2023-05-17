package com.zhang.mapreduce.partitionerAndWritableComparable;

import org.apache.hadoop.mapreduce.Partitioner;
import org.w3c.dom.Text;

/**
 * @author 张永锐
 * Date  2022/8/19 15:07
 * Describe
 */
public class FlowPartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        // text 是手机号
        String phoneNum = text.toString();
        // 获取前三位
        String prePhoneNum = phoneNum.substring(0, 3);

        // 分区号
        int partition;

        // 设置分区
        if("136".equals(prePhoneNum)){
            partition = 0;
        } else if("137".equals(prePhoneNum)) {
            partition = 1;
        } else if("138".equals(prePhoneNum)) {
            partition = 2;
        } else if("139".equals(prePhoneNum)) {
            partition = 3;
        } else {
            partition = 4;
        }

        // 返回分区号
        return partition;
    }
}
