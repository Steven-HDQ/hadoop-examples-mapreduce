package com.opstty.q7;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MostTreeReducer2 extends Reducer<NullWritable, MyWritable, Text, NullWritable> {
    private Text dist = new Text();

    public void reduce(NullWritable key, Iterable<MyWritable> values, Context context)
            throws IOException, InterruptedException {
        int max = 0;
        String district = "";
        for (MyWritable val : values) {
            if (val.getcount() > max) {
                max = val.getcount();
                district = val.getdist();
            }
        }
        dist.set(district);
        context.write(dist, NullWritable.get());
    }
}

