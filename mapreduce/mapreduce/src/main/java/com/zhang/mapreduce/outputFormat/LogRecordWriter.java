package com.zhang.mapreduce.outputFormat;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author 张永锐
 * Date  2022/8/19 16:04
 * Describe
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream study;
    private FSDataOutputStream other;

    public LogRecordWriter(TaskAttemptContext job) {
        // 创建两个流，一个流存喊study的数据，一个流存其他的不含study的数据
        try {
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());
            study = fileSystem.create(new Path("D:\\桌面\\study.log"));
            other = fileSystem.create(new Path("D:\\桌面\\other.log"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String log = text.toString();
        // 具体写
        if (log.contains("study")){
            study.writeBytes(log + "\n");
        } else {
            other.writeBytes(log + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 关流
        IOUtils.close(study);
        IOUtils.close(other);
    }
}
