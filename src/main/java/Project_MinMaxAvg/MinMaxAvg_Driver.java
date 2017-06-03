/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_MinMaxAvg;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author deepali
 */
public class MinMaxAvg_Driver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "MinMaxAvg_Driver");
        job.setJarByClass(MinMaxAvg_Driver.class);
        job.setMapperClass(MinMaxAvgMapper.class);
        job.setReducerClass(MinMaxAvgReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(MinMaxAvgCustom.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(MinMaxAvgCustom.class);
//        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
