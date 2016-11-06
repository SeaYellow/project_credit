package com.merit.mr;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2016/9/9.
 */
public class CreditCountWritable implements WritableComparable<CreditCountWritable> {
    private int count;

    public void setCount(int count) {
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    @Override
    public int compareTo(CreditCountWritable c) {
        return c.getCount() - getCount();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(getCount());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        setCount(dataInput.readInt());
    }
}
