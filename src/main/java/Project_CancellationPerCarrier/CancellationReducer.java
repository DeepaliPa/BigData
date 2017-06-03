/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_CancellationPerCarrier;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author deepali
 */
public class CancellationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable res = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
            Context context
    ) throws IOException, InterruptedException {

        int total = 0;
        for (IntWritable val : values) {
            total += val.get();
        }
        res.set(total);

        context.write(key, res);
    }

}
