package com.opstty.q1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class DistrictsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final static int dist_col = 1;
    private Text dist = new Text();

    protected void map(LongWritable key, Text value, Context context)
            throws IOException,InterruptedException {
        // Ignoring header
        if (key.get()!=0) {
            String line = value.toString();
            String[] splitted = line.split(";");
            String typeString = splitted[dist_col];
            dist.set(typeString);
            context.write(dist, one);
        }
    }
}