package com.opstty.q7;

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
public class MostTreeReducer2Test {
    @Mock
    private Reducer.Context context;
    private MostTreeReducer2 mostTreeReducer2;

    @Before
    public void setup() {
        this.mostTreeReducer2 = new MostTreeReducer2();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        Iterable<MyWritable> values = Arrays.asList(new MyWritable("12",16),new MyWritable("16",36));
        this.mostTreeReducer2.reduce(NullWritable.get(), values, this.context);
        verify(this.context).write(new Text("16"), NullWritable.get());
    }
}