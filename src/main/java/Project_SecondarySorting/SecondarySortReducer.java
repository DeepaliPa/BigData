/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_SecondarySorting;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author deepali
 */
public class SecondarySortReducer extends Reducer<CompositeGroupKey, LongWritable, LongWritable,CompositeGroupKey> {

    public void reduce(CompositeGroupKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;

        for (LongWritable val : values) {
            sum += val.get();
        }

        context.write(new LongWritable(sum),key);

    }
}
