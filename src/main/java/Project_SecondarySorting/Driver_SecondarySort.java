/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_SecondarySorting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author deepali
 */
public class Driver_SecondarySort {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "SecondarySort");
            job.setJarByClass(Driver_SecondarySort.class);

            job.setMapperClass(SecondarySortMapper.class);
            job.setMapOutputKeyClass(CompositeGroupKey.class);
            job.setMapOutputValueClass(LongWritable.class);

            job.setReducerClass(SecondarySortReducer.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(CompositeGroupKey.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
