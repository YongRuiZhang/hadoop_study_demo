package com.zhang.mapreduce.compression;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输入：
 *      KEY ： LongWritable  // 第几行
 *      VALUE ： Text        // 该行的内容
 * 输出：
 *      KEY ： Text          // 被统计的单词
 *      VALUE ： IntWritable // 该单词的数量
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outKEY = new Text();
    private IntWritable outVALUE = new IntWritable(1); // 一个单词数量是 1

    /**
     * 重写map方法
     * @param key 输入数据的KEY
     * @param value 输入数据的VALUE
     * @param context 充当上下文，进行map和reduce之间的联络
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {

        // 1.获取一行 zyr zyr zyr
        String line = value.toString();

        // 2.切割 [zyr,zyr,zyr]
        String[] words = line.split(" ");

        // 3.循环写出
        for (String word : words) {
            // 封装outKEY
            outKEY.set(word);
            // 写出
            context.write(outKEY,outVALUE);
        }

    }
}
