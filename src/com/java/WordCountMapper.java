package com.java;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text map_key = new Text();
    IntWritable map_value = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //读取一行
        String line = value.toString();

        //
        String[] words = line.split(" ");

        //循环写出
        for (String word : words) {

            map_key.set(word);
            context.write(map_key, map_value);
        }
    }
}
