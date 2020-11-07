package com.opstty.q7;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MostTreeMapper2 extends Mapper<Object, Text, NullWritable, MyWritable> {
    private MyWritable mydata = new MyWritable("0",0);

    protected void map(Object key, Text value, Context context)
            throws IOException,InterruptedException {
        String line = value.toString();
        String[] splitted = line.split("\t");

        try {
            mydata.set(splitted[0],Integer.parseInt(splitted[1]));
            context.write(NullWritable.get(),mydata);
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }
}
