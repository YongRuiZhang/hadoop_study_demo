package com.zhang.mapreduce.mapJoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    HashMap<String, String> pdMap = new HashMap<>();
    private Text outKey = new Text();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 获取缓存文件，并把缓存文件内容封装到集合 pd.txt中
        URI[] cacheFiles = context.getCacheFiles();

        // 获取流
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));

        // 从流中读取数据
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())){
            // 切割 [01,小米]
            String[] split = line.split("\t");
            pdMap.put(split[0],split[1]);
        }
        // 关流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 处理 order.txt
        String line = value.toString();
        // 分割 [1001,01,1]
        String[] fields = line.split("\t");
        // 获取pid
        String pName = pdMap.get(fields[1]);
        // 封装kv
        outKey.set(fields[0] + "\t" + pName + "\t" + fields[2]);
        context.write(outKey, NullWritable.get());
    }
}
