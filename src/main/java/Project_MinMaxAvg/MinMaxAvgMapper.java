/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_MinMaxAvg;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author deepali
 */
public class MinMaxAvgMapper extends Mapper<LongWritable, Text, LongWritable, MinMaxAvgCustom> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            String val = value.toString();

            String[] result = val.split("\t");
            String[] tempValues = result[1].split(",");
            String flightNumber = tempValues[9];

            if (tempValues[14].trim().equals("NA")) {

                tempValues[14] = "0";
            }
            if (tempValues[15].trim().equals("NA")) {

                tempValues[15] = "0";
            }
            if (tempValues[9].trim().equals("NA")) {

                tempValues[9] = "0";
            }
            String count = "1";

            MinMaxAvgCustom cust = new MinMaxAvgCustom(tempValues[14], tempValues[14], tempValues[15], tempValues[15], count);

            context.write(new LongWritable(Integer.parseInt(flightNumber)), cust);

        } catch (Exception ex) {

            ex.printStackTrace();

        }

    }

}
