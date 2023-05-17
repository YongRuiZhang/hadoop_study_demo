package com.zhang.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 这里和Mapper中的输入输出刚好相反
 * 输入：
 *      KEY ： Text          // 被统计的单词
 *      VALUE ： IntWritable // 该单词的数量
 * 输出：
 *      KEY ： Text          // 被统计的单词
 *      VALUE ： IntWritable // 该单词的数量
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable outValue = new IntWritable();

    /**
     * 重写reduce
     * @param key   输入的key
     * @param values    输入的value，是一个可迭代对象(不是迭代器，它时一个集合，如果想要遍历它，需要先用value.iterator()得到一个迭代器，或者增强for循环，集合的遍历方式都可)
     *      例如zyr zyr zyr,被mapper弄成了三个，所以传入的K-V实际上是三组 zyr-1。但是Reducer在接收数据时会转化为：zyr-(1,1,1)
     * @param context   上下文作用
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        // 累加  zyr-(1,1,1)
        for (IntWritable value : values) {
            sum += value.get();
        }

        outValue.set(sum);

        // 写出
        context.write(key,outValue);

    }
}
