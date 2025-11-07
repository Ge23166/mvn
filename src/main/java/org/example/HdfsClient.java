package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
public class HdfsClient {

    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();

        // FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration);
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration,"root");

        // 2 创建目录
        fs.mkdirs(new Path("/xiyou/huaguoshan/"));

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "root");

        // 2 上传文件
        fs.copyFromLocalFile(new Path("d:/sunwukong.txt"), new Path("/xiyou/huaguoshan"));

        // 3 关闭资源
        fs.close();
    }
        @Test
        public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException{

            // 1 获取文件系统
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "root");

            // 2 执行下载操作
            // boolean delSrc 指是否将原文件删除
            // Path src 指要下载的文件路径
            // Path dst 指将文件下载到的路径
            // boolean useRawLocalFileSystem 是否开启文件校验
            fs.copyToLocalFile(false, new Path("/xiyou/huaguoshan/sunwukong.txt"), new Path("d:/sunwukong4.txt"), true);

            // 3 关闭资源
            fs.close();
        }

    }
