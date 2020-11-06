package com.opstty.q6;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<Text, MyWritable, Text, NullWritable> {
    private Text dist = new Text();

    public void reduce(Text key, Iterable<MyWritable> values, Context context)
            throws IOException, InterruptedException {
        float oldestyear = 9999;
        String district = "";
        for (MyWritable val : values) {
            if (val.getyear() < oldestyear) {
                oldestyear = val.getyear();
                district = val.getdist();
            }
        }
        dist.set(district);
        context.write(dist, NullWritable.get());
    }
}

