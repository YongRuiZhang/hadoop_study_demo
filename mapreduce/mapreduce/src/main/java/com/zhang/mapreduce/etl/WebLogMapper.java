package com.zhang.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 1. 获取一行
        String line = value.toString();
        // 3. ETL
        boolean result = parseLog(line,context);
        if (!result) {
            return;
        }
        // 4. 写出
        context.write(value, NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        // 2. 切割
        String[] split = line.split(" ");
        // 判断日志长度是否大于11
        if(split.length > 11) {
            return true;
        }
        return false;
    }
}
