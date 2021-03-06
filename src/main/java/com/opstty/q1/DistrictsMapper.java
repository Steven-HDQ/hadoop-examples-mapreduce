package com.opstty.q1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class DistrictsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
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
            context.write(dist, NullWritable.get());
        }
    }
}