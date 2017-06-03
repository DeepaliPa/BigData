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
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author deepali
 */
public class MinMaxAvgReducer extends Reducer<LongWritable, MinMaxAvgCustom, LongWritable, MinMaxAvgCustom> {

    MinMaxAvgCustom minMaxAvgCustom = new MinMaxAvgCustom();

    public void reduce(LongWritable key, Iterable<MinMaxAvgCustom> values, Context context) throws IOException, InterruptedException {

        int minArrDelay = Integer.MAX_VALUE;
        int maxArrDelay = Integer.MIN_VALUE;
        int minDepDelay = Integer.MAX_VALUE;
        int maxDepDelay = Integer.MIN_VALUE;
        int sum = 0;

        for (MinMaxAvgCustom val : values) {

            if (minArrDelay > Integer.parseInt(val.getMinArrDelay())) {
                minArrDelay = Integer.parseInt(val.getMinArrDelay());
            }

            if (maxArrDelay < Integer.parseInt(val.getMaxArrDelay())) {
                maxArrDelay = Integer.parseInt(val.getMaxArrDelay());
            }

            if (minDepDelay > Integer.parseInt(val.getMinDepDelay())) {
                minDepDelay = Integer.parseInt(val.getMinDepDelay());
            }

            if (maxDepDelay < Integer.parseInt(val.getMaxDepDelay())) {
                maxDepDelay = Integer.parseInt(val.getMaxDepDelay());
            }

            sum += Integer.parseInt(val.getCount());

        }

        context.write(key, new MinMaxAvgCustom(String.valueOf(minArrDelay), String.valueOf(maxArrDelay), String.valueOf(minDepDelay), String.valueOf(maxDepDelay), String.valueOf(sum)));

    }

}
