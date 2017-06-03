/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_Chaining;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author deepali
 */
public class Chaining {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String headvalue = value.toString();
        try {               
            if(headvalue.contains("UniqueCarrier")){
            return;
            }   
            
            String[] result = headvalue.split(",");            
            if(result[21].equals("0")){
            
                Text airport = new Text();
                airport.set(result[16]);
                context.write(airport, one);
                
            }
            } catch (Exception e) {
                System.out.println( e.getMessage());
            }
        }
    }
    
    
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
                 int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);

            context.write(key, result);
        }
    }
    
    
    public static class TokenizerMapper1
            extends Mapper<Object, Text, DoubleWritable, Text> 
    {
                
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String headvalue = value.toString();
            String[] result = headvalue.split("\t");            
                
                Text top10Carrier = new Text();
                top10Carrier.set(result[0]);
            
                DoubleWritable count = new DoubleWritable(Double.parseDouble(result[1]));

                context.write(count,top10Carrier);
                
        }

    }

    public static class TopTenReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
                
            private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();
    private static int cnt = 1;
            
            public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                if(cnt < 11){
                for (Text value : values) {
                context.write(value, key);
                
                }     
                
                }
                this.cnt++;
                
            }
            
            
     }
            
public static class ChainJobs extends Configured implements Tool {

        private static String OUTPUT_PATH = "";

        @Override
        public int run(String[] strings) throws Exception {

            OUTPUT_PATH = strings[1];
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "Project_Chaining");

            job.setJarByClass(Chaining.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            TextInputFormat.addInputPath(job, new Path(strings[0]));
            TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

            job.waitForCompletion(true);

            // job1
            Job job1 = Job.getInstance(conf, "Project_Chaining");

            job1.setJarByClass(Chaining.class);
            job1.setMapperClass(TokenizerMapper1.class);
            job1.setMapOutputKeyClass(DoubleWritable.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setReducerClass(TopTenReducer.class);
            
            job1.setSortComparatorClass(DescendingIntComparator.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(DoubleWritable.class);

            job1.setSortComparatorClass(LongWritable.DecreasingComparator.class);
            TextInputFormat.addInputPath(job1, new Path(OUTPUT_PATH));
            TextOutputFormat.setOutputPath(job1, new Path(strings[2]));

            return job1.waitForCompletion(true) ? 0 : 1;

        }
    }            
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
               if (args.length != 3) {
            System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
            System.exit(0);
        }
        ToolRunner.run(new Configuration(), new ChainJobs(), args);
    }
    
     public static class DescendingIntComparator extends WritableComparator {

        public DescendingIntComparator() {
            super(DoubleWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

            Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
            Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

            return (v1.compareTo(v2)) * -1;

        }
    }
    
}

