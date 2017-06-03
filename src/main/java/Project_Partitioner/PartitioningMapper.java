/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_Partitioner;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author deepali
 */
public class PartitioningMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {

        try {
            String headerValue = value.toString();

            if (headerValue.contains("CRSDepTime")) {
                return;
            }

            String[] result = value.toString().split(",");
            String month = result[1].trim();
            context.write(new Text(month), value);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
