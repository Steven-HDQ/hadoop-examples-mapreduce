package com.opstty.q6;

import org.apache.hadoop.io.LongWritable;
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
public class OldestTreeMapperTest {
    @Mock
    private Mapper.Context context;
    private OldestTreeMapper oldestTreeMapper;

    @Before
    public void setup() {
        this.oldestTreeMapper = new OldestTreeMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "(48.8717782491, 2.27973325759);16;Aesculus;hippocastanum;Sapindaceae;1999;30.0;505.0;Avenue Foch;Marronnier d'Inde;;30;Avenue Foch" ;
        this.oldestTreeMapper.map(new LongWritable(2), new Text(value), this.context);
        verify(this.context, times(1))
                .write(new Text("key"), new MyWritable(1999,"16"));
    }
}

