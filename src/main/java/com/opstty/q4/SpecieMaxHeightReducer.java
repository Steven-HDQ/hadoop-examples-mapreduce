package com.opstty.q4;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SpecieMaxHeightReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable maxHeight = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float currentMaxHeight = 0;
        for (FloatWritable val : values) {
            if (val.get() > currentMaxHeight) {
                currentMaxHeight = val.get();
            }
        }
        maxHeight.set(currentMaxHeight);
        context.write(key, maxHeight);
    }
}

