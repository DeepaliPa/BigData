/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_MinMaxAvg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 * @author deepali
 */
public class MinMaxAvgCustom implements Writable {

    String minArrDelay;
    String maxArrDelay;
    String minDepDelay;
    String maxDepDelay;
    String count;

    public MinMaxAvgCustom() {
    }

    public MinMaxAvgCustom(String minArrDelay, String maxArrDelay, String minDepDelay, String maxDepDelay, String count) {
        this.minArrDelay = minArrDelay;
        this.maxArrDelay = maxArrDelay;
        this.minDepDelay = minDepDelay;
        this.maxDepDelay = maxDepDelay;
        this.count = count;
    }

    public void readFields(DataInput dataInput) throws IOException {
        minArrDelay = WritableUtils.readString(dataInput);
        maxArrDelay = WritableUtils.readString(dataInput);
        minDepDelay = WritableUtils.readString(dataInput);
        maxDepDelay = WritableUtils.readString(dataInput);
        count = WritableUtils.readString(dataInput);

    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, minArrDelay);
        WritableUtils.writeString(dataOutput, maxArrDelay);
        WritableUtils.writeString(dataOutput, minDepDelay);
        WritableUtils.writeString(dataOutput, maxDepDelay);
        WritableUtils.writeString(dataOutput, count);
    }

    public String getMinArrDelay() {
        return minArrDelay;
    }

    public void setMinArrDelay(String minArrDelay) {
        this.minArrDelay = minArrDelay;
    }

    public String getMaxArrDelay() {
        return maxArrDelay;
    }

    public void setMaxArrDelay(String maxArrDelay) {
        this.maxArrDelay = maxArrDelay;
    }

    public String getMinDepDelay() {
        return minDepDelay;
    }

    public void setMinDepDelay(String minDepDelay) {
        this.minDepDelay = minDepDelay;
    }

    public String getMaxDepDelay() {
        return maxDepDelay;
    }

    public void setMaxDepDelay(String maxDepDelay) {
        this.maxDepDelay = maxDepDelay;
    }

    public String getCount() {
        return count;
    }

    public void setCount(String count) {
        this.count = count;
    }
    
    

    @Override
    public String toString() {
        return (new StringBuilder().append(minArrDelay).append("\t").append(maxArrDelay).append("\t").append(minDepDelay).append("\t").append(maxDepDelay).append("\t").append(count).append("\t")).toString();
    }

}
