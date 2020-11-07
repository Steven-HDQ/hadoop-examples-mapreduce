package com.opstty.q7;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MostTreeMapper2Test {
    @Mock
    private Mapper.Context context;
    private MostTreeMapper2 mostTreeMapper2;

    @Before
    public void setup() {
        this.mostTreeMapper2 = new MostTreeMapper2();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "16\t36";
        this.mostTreeMapper2.map(null, new Text(value),this.context);
        verify(this.context, times(1))
                .write(NullWritable.get(), new MyWritable("16",36));
    }
}

