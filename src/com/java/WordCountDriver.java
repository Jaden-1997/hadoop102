package com.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    //main方法入口
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        //1.获得Job对象

        Job job = Job.getInstance(conf);

        //2.设置jar存储位置：指定Driver类
        job.setJarByClass(WordCountDriver.class);

        //3.关联map和Reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        //4.设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.设置输入路径和输出路径lib包
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.提交job
        //打印一些日志信息
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
