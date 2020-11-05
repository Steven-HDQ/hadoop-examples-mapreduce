package com.opstty.q3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpecieCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final static int spec_col = 2;
    private Text spec = new Text();

    protected void map(LongWritable key, Text value, Context context)
            throws IOException,InterruptedException {
        // Ignoring header
        if (key.get()!=0) {
            String line = value.toString();
            String[] splitted = line.split(";");
            String typeString = splitted[spec_col];
            spec.set(typeString);
            context.write(spec, one);
        }
    }
}
