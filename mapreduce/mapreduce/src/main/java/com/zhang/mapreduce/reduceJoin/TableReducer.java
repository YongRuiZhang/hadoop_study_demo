package com.zhang.mapreduce.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context)
            throws IOException, InterruptedException {
        // key : 01
        // value :
            // 01   1001    1   order
            // 01   1004    4   order
            // 01   小米         pd

        // 准备初始化集合
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();

        // 循环遍历
        for (TableBean value : values) {
            if("order".equals(value.getFlag())){ // 处理订单表
                TableBean tempTableBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tempTableBean, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }

                orderBeans.add(tempTableBean);
            } else { // 处理商品表
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }

            }
        }
        // 循环遍历 orderBeans ,赋值 name
        for (TableBean orderBean : orderBeans) {
            orderBean.setName(pdBean.getName());
            // 写出
            context.write(orderBean, NullWritable.get());
        }
    }
}
