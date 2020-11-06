package com.opstty.q6;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeMapper extends Mapper<LongWritable, Text, Text, MyWritable> {
    private Text keyE = new Text("key");
    private MyWritable mydata = new MyWritable(9999,"0");
    private final static int dist_col = 1;
    private final static int year_col = 5;

    protected void map(LongWritable key, Text value, Context context)
            throws IOException,InterruptedException {
        // Ignoring header
        if (key.get()!=0) {
            String line = value.toString();
            String[] splitted = line.split(";");

            try {
                mydata.set(Integer.parseInt(splitted[year_col]), splitted[dist_col]);
                context.write(keyE,mydata);
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }
    }
}
