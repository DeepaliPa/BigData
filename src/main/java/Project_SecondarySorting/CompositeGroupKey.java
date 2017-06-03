/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_SecondarySorting;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 * @author deepali
 */
public class CompositeGroupKey implements WritableComparable<CompositeGroupKey> {

    String origin;
    String destination;

    public CompositeGroupKey() {
    }

    public CompositeGroupKey(String origin, String destination) {
        this.origin = origin;
        this.destination = destination;
    }

    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, origin);
        WritableUtils.writeString(out, destination);
    }

    public void readFields(DataInput in) throws IOException {
        this.origin = WritableUtils.readString(in);
        this.destination = WritableUtils.readString(in);
    }

    public int compareTo(CompositeGroupKey pop) {
        if (pop == null) {
            return 0;
        }
        int intcnt = origin.compareTo(pop.origin);
        return intcnt == 0 ? destination.compareTo(pop.destination) : intcnt;
    }

    @Override
    public String toString() {
        return origin.toString() + ":" + destination.toString();
    }

}
