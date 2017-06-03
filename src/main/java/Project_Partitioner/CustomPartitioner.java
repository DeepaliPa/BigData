/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_Partitioner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author deepali
 */
public class CustomPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numOfPartitions) {
        String month = key.toString();

        if (month.equalsIgnoreCase("1")) {
            return 0;
        }
        if (month.equalsIgnoreCase("2")) {
            return 1;
        }
        if (month.equalsIgnoreCase("3")) {
            return 2;
        }
        if (month.equalsIgnoreCase("4")) {
            return 3;
        }
        if (month.equalsIgnoreCase("5")) {
            return 4;
        }
        if (month.equalsIgnoreCase("6")) {
            return 5;
        }
        if (month.equalsIgnoreCase("7")) {
            return 6;
        }
        if (month.equalsIgnoreCase("8")) {
            return 7;
        }
        if (month.equalsIgnoreCase("9")) {
            return 8;
        }
        if (month.equalsIgnoreCase("10")) {
            return 9;
        }
        if (month.equalsIgnoreCase("11")) {
            return 10;
        }
        if (month.equalsIgnoreCase("12")) {
            return 11;
        }
        return 0;
    }

}
