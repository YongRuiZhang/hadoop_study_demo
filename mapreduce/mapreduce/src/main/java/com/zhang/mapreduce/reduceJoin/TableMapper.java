package com.zhang.mapreduce.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private String fileName;
    private Text outKey = new Text();
    private TableBean outValue = new TableBean();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        // 初始化 order.txt  pd.txt
        FileSplit split = (FileSplit) context.getInputSplit();

        fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context)
            throws IOException, InterruptedException {
        // 1. 获取一行
        String line = value.toString();
        // 2. 判断是哪一个文件的
        if (fileName.contains("order")) { // 处理的是 order.txt
            // 3. 分割 [1001,01,1]
            String[] split = line.split("\t");
            // 4. 封装kv
            outKey.set(split[1]);
            outValue.setId(split[0]);
            outValue.setPid(split[1]);
            outValue.setAmount(Integer.parseInt(split[2]));
            outValue.setName("");
            outValue.setFlag("order");
        } else { // 处理的是 pd.txt
            // 3. 分割 [01,小米]
            String[] split = line.split("\t");
            // 4. 封装
            outKey.set(split[0]);
            outValue.setId("");
            outValue.setPid(split[0]);
            outValue.setAmount(0);
            outValue.setName(split[1]);
            outValue.setFlag("pd");
        }
        // 5.写出
        context.write(outKey, outValue);
    }
}
