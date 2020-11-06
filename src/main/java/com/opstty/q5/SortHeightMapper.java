package com.opstty.q5;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortHeightMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {
    private FloatWritable height = new FloatWritable();
    private final static int objectid_col = 11;
    private final static int height_col = 6;
    private Text objectid = new Text();

    protected void map(LongWritable key, Text value, Context context)
            throws IOException,InterruptedException {
        // Ignoring header
        if (key.get()!=0) {
            String line = value.toString();
            String[] splitted = line.split(";");

            try {
                height.set(Float.parseFloat(splitted[height_col]));
                objectid.set(splitted[objectid_col]);
                context.write(height,objectid);
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }
    }
}
