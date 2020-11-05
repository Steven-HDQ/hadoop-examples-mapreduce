package com.opstty.q1;

import com.opstty.q1.DistrictsReducer;
import org.apache.hadoop.io.IntWritable;
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
public class DistrictsReducerTest {
    @Mock
    private Reducer.Context context;
    private DistrictsReducer districtsReducer;

    @Before
    public void setup() {
        this.districtsReducer = new DistrictsReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "key";
        IntWritable value = new IntWritable(1);
        Iterable<IntWritable> values = Arrays.asList(value, value, value);
        this.districtsReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key), new IntWritable(0));
    }
}