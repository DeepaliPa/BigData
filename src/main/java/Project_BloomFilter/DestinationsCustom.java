/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_BloomFilter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 * @author deepali
 */
public class DestinationsCustom implements Writable {

//    String month;
//    String origin;
    String destination;
//    String flightNo;
//    String count;

    public DestinationsCustom() {
    }

    public DestinationsCustom( String destination) {
      // this.month = month;
       // this.origin = origin;
        this.destination = destination;
      //  this.flightNo = flightNo;
      //  this.count = count;
    }

    public void write(DataOutput dataOutput) throws IOException {
//        WritableUtils.writeString(dataOutput, month);
//        WritableUtils.writeString(dataOutput, origin);
       WritableUtils.writeString(dataOutput, destination);
//        WritableUtils.writeString(dataOutput, flightNo);
//        WritableUtils.writeString(dataOutput, count);
    }

    public void readFields(DataInput dataInput) throws IOException {
//        month = WritableUtils.readString(dataInput);
//        origin = WritableUtils.readString(dataInput);
        destination = WritableUtils.readString(dataInput);
//        flightNo = WritableUtils.readString(dataInput);
//        count = WritableUtils.readString(dataInput);
    }

  

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }


    @Override
    public String toString() {
        return (new StringBuilder().append(destination)).toString();
    }

}
