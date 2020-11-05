package com.opstty.q4;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpecieMaxHeightMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private FloatWritable height = new FloatWritable();
    private final static int spec_col = 2;
    private final static int height_col = 6;
    private Text spec = new Text();

    protected void map(LongWritable key, Text value, Context context)
            throws IOException,InterruptedException {
        // Ignoring header
        if (key.get()!=0) {
            String line = value.toString();
            String[] splitted = line.split(";");

            try {
                height.set(Float.parseFloat(splitted[height_col]));
                spec.set(splitted[spec_col]);
                context.write(spec, height);
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }
    }
}
