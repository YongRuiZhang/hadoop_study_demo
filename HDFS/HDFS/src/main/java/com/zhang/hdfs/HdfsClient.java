package com.zhang.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @author 张永锐
 * Date  2022/8/16 22:31
 * Describe
 */
public class HdfsClient {

    private FileSystem fileSystem;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 连接集群的NameNode地址
        URI uri = new URI("hdfs://hadoop103:8020");
        // 配置文件
        Configuration configuration = new Configuration();
        // 用户
        String user = "hadoop103";
        // 1. 获取到客户端对象
        fileSystem = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        // 3. 关闭资源
        fileSystem.close();
    }

    // 创建文件夹
    @Test
    public void testMkdirs() throws IOException {

        // 2. 创建一给文件夹 西游 下面一个 花果山
        fileSystem.mkdirs(new Path("/xiyou/huaguoshan1")); // 注意参数是一个Path对象，创建对象，用构造函数赋值

    }

    // 测试上传
    @Test
    public void testPut() throws IOException {
        // 参数一：是否删除原数据，参数二：是否允许覆盖，参数三：源数据路径（Win），参数四：目的地路径（HDFS）
        fileSystem.copyFromLocalFile(false,false,
                new Path("E:\\sunwukong.txt"),
                new Path("hdfs://hadoop103/xiyou/huaguoshan") );//加不加协议头都无所谓，实际开发不加，直接写/xiyou/huaguoshan
    }

    // 测试下载
    @Test
    public void testGet() throws IOException {
        // 参数一：是否删除原数据，参数二：源数据路径（HDFS），参数三：目的地路径（Win）,参数四：
        fileSystem.copyToLocalFile(false,new Path("/xiyou/huaguoshan"),new Path("E:\\sunwukong.txt"),false);
    }

    // 测试文件删除
    @Test
    public void testDelete() throws IOException {
        // 参数一：三处文件的地址，参数二：是否递归删除
        fileSystem.delete(new Path("/jinguo"),true); // 一个文件，用不用递归都一样
    }

    // 文件的移动和更名
    @Test
    public void testmv() throws IOException {
        // 参数一：原文件路径，参数二：目标文件路径
        // 文件的更名
//        fileSystem.rename(new Path("/xiyou/huaguoshan/sunwukong.txt"),
//                new Path("/xiyou/huaguoshan/qitiandasheng.txt"));

        // 文件移动和更名，又要移动，又要更名
//        fileSystem.rename(new Path("/input/word.txt"),new Path("/word_mv.txt"));

        // 目录的更名
        fileSystem.rename(new Path("/input"), new Path("/output"));
    }

    // 获取文件详细信息
    @Test
    public void fileDetail () throws IOException {
        // 参数一：查看的路径，参数二：是否递归。返回一个迭代器
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        // 遍历文件
        while (listFiles.hasNext()) {
            System.out.println("===============================================");
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("Path : " + fileStatus.getPath());
            System.out.println("Name : " + fileStatus.getPath().getName());
            System.out.println("Permission : " + fileStatus.getPermission());
            System.out.println("Owner : " + fileStatus.getOwner());
            System.out.println("Group : " + fileStatus.getGroup());
            System.out.println("Len : " + fileStatus.getLen());
            System.out.println("ModificationTime : " + fileStatus.getModificationTime());
            System.out.println("Replication : " + fileStatus.getReplication());
            System.out.println("BlockSize : " + fileStatus.getBlockSize());
            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
            System.out.println("===============================================");
        }
    }

    // 判断是文件夹还是文件
    @Test
    public void testFile() throws IOException {
        FileStatus[] listStatus = fileSystem.listStatus(new Path("/"));
        for (FileStatus status : listStatus) {
            if (status.isFile()) {
                System.out.println(status.getPath().getName() + "是一个文件");
            } else {
                System.out.println(status.getPath().getName() + "是一个目录");
            }
        }
    }
}
