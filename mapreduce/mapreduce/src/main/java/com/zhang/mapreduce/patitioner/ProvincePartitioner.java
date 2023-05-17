package com.zhang.mapreduce.patitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
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
