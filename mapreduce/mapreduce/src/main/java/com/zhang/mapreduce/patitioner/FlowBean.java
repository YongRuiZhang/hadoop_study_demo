package com.zhang.mapreduce.patitioner;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1. 实现writable接口
 * 2. 重写序列化和反序列化方法
 * 3. 重写空参构造
 * 4. 重写toString方法
 */
public class FlowBean implements Writable {

    private Long upFlow; // 上行流量
    private Long downFlow; // 下行流量
    private Long sumFlow; // 总流量

    // 空参构造
    public FlowBean() {
    }

    /**
     * 重写序列化方法
     * @param dataOutput
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // 序列化的顺序无所谓，但是要保证序列化和反序列化的顺序一致
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     * 重写反序列化方法
     * @param dataInput
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    // 重写toString()方法
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }
}
