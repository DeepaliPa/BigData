/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_SecondarySorting;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author deepali
 */
public class SecondarySortMapper extends Mapper<LongWritable,Text, CompositeGroupKey, LongWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            String val = value.toString();

            String[] result = val.split("\t");
            String[] tempValues = result[1].split(",");

            String count = "1";

            CompositeGroupKey compKeys = new CompositeGroupKey(tempValues[16], tempValues[17]);

            context.write(compKeys, new LongWritable(Integer.parseInt(count)));

        } catch (Exception ex) {

            ex.printStackTrace();

        }

    }
}
