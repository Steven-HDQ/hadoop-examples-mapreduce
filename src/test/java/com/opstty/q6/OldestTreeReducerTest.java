package com.opstty.q6;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
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
public class OldestTreeReducerTest {
    @Mock
    private Reducer.Context context;
    private OldestTreeReducer oldestTreeReducer;

    @Before
    public void setup() {
        this.oldestTreeReducer = new OldestTreeReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "key";
        Iterable<MyWritable> values = Arrays.asList(new MyWritable(1999,"13"),new MyWritable(1988,"20"));
        this.oldestTreeReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text("20"), NullWritable.get());
    }
}