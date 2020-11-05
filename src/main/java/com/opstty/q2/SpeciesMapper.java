package com.opstty.q2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeciesMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
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
            context.write(spec, NullWritable.get());
        }
    }
}