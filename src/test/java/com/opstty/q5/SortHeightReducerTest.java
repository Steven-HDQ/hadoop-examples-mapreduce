package com.opstty.q5;

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
public class SortHeightReducerTest {
    @Mock
    private Reducer.Context context;
    private SortHeightReducer sortHeightReducer;

    @Before
    public void setup() {
        this.sortHeightReducer = new SortHeightReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        float key = 30;
        Iterable<Text> values = Arrays.asList(new Text("4"),new Text("5"));
        this.sortHeightReducer.reduce(new FloatWritable(key), values, this.context);
        verify(this.context).write(new FloatWritable(key), new Text("4"));
        verify(this.context).write(new FloatWritable(key), new Text("5"));
    }
}