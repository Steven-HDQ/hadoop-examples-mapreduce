package com.opstty.q7;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyWritable implements WritableComparable<MyWritable> {
    private String district;
    private int count;

    public MyWritable() {}

    public MyWritable(String dist,int count){
        this.district=dist;
        this.count=count;
    }

    public void set(String district,int count){
        this.district=district;
        this.count=count;
    }

    public String getdist() {
        return district;
    }

    public int getcount() {
        return count;
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj, "district","count");
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this, "district","count");
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(district);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.district = dataInput.readUTF();
        this.count = dataInput.readInt();
    }

    public int compareTo(MyWritable o) {
        return o.count-this.count;
    }
}

