/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_TopTen;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author deepali
 */
public class TopTenPatternClass {

    public static class TopTenPatternMapper extends Mapper<Object, Text, Text, Text> {

        private TreeMap<Integer, Text> distoTopMap = new TreeMap<Integer, Text>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            try {
                String val = value.toString();

                String[] result = val.split("\t");

                distoTopMap.put(Integer.parseInt(result[0]), new Text(result[1]));

                if (distoTopMap.size() > 10) {
                    distoTopMap.remove(distoTopMap.firstKey());
                }

            } catch (Exception ex) {

                ex.printStackTrace();

            }

        }

        public void cleanup(Context context) throws IOException, InterruptedException {

            for (int k : distoTopMap.descendingMap().keySet()) {

                context.write(new Text(distoTopMap.get(k)), new Text(String.valueOf(k)));

            }

        }

    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "TopTenPattern");
            job.setJarByClass(TopTenPatternClass.class);

            job.setMapperClass(TopTenPatternMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setNumReduceTasks(0);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
