package com.opstty.q2;

import org.apache.hadoop.io.NullWritable;
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
public class SpeciesMapperTest {
    @Mock
    private Mapper.Context context;
    private SpeciesMapper speciesMapper;

    @Before
    public void setup() {
        this.speciesMapper = new SpeciesMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "GEOPOINT;ARRONDISSEMENT;GENRE;ESPECE;FAMILLE;ANNEE PLANTATION;HAUTEUR;CIRCONFERENCE;ADRESSE;NOM COMMUN;VARIETE;OBJECTID;NOM_EV" ;
        this.speciesMapper.map(new LongWritable(2), new Text(value), this.context);
        verify(this.context, times(1))
                .write(new Text("GENRE"), NullWritable.get());
    }
}

