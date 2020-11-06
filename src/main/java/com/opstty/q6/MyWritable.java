package com.opstty.q6;

import org.apache.hadoop.io.WritableComparable;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyWritable implements WritableComparable<MyWritable>{
    private int plantyear;
    private String district;

    public MyWritable() {}

    public MyWritable(int year,String dist){
        this.plantyear=year;
        this.district=dist;
    }

    public void set(int plantyear,String district){
        this.plantyear=plantyear;
        this.district=district;
    }

    public int getyear() {
        return plantyear;
    }

    public String getdist() {
        return district;
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj, "plantyear", "district");
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this, "plantyear", "district");
    }

//    @Override
//    public boolean equals(Object o) {
//        if (!(o instanceof MyWritable))
//            return false;
//        MyWritable other = (MyWritable)o;
//        return (this.plantyear == other.plantyear)&&(this.district == other.district);
//    }
//
//    @Override
//    public int hashCode() {
//        int result = district.hashCode();
//        result = 17 * result + plantyear;
//        return result;
//    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(plantyear);
        dataOutput.writeUTF(district);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.plantyear = dataInput.readInt();
        this.district = dataInput.readUTF();
    }

    public int compareTo(MyWritable o) {
        return o.plantyear-this.plantyear;
    }
}

