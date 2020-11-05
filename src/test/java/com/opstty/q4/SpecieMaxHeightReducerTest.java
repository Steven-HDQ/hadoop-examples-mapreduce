package com.opstty.q4;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SpecieMaxHeightReducerTest {
    @Mock
    private Reducer.Context context;
    private SpecieMaxHeightReducer specieMaxHeightReducer;

    @Before
    public void setup() {
        this.specieMaxHeightReducer = new SpecieMaxHeightReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "key";
        Iterable<FloatWritable> values = Arrays.asList(new FloatWritable(4), new FloatWritable(5), new FloatWritable(3));
        this.specieMaxHeightReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key), new FloatWritable(5));
    }
}