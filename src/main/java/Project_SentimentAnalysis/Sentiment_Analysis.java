/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_SentimentAnalysis;

import java.io.BufferedReader;

import java.io.IOException;

import java.io.InputStreamReader;

import java.net.URI;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.filecache.DistributedCache;

import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Mapper.Context;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;

import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author deepali
 */
public class Sentiment_Analysis extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private URI[] files;

        private HashMap<String, String> AFINN_map = new HashMap<String, String>();

        @Override

        public void setup(Context context) throws IOException {

            files = DistributedCache.getCacheFiles(context.getConfiguration());

            System.out.println("files:" + files);

            Path path = new Path(files[0]);

            FileSystem fs = FileSystem.get(context.getConfiguration());

            FSDataInputStream in = fs.open(path);

            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            String line = "";

            while ((line = br.readLine()) != null) {

                String splits[] = line.split("\t");

                AFINN_map.put(splits[0], splits[1]);

            }

            br.close();

            in.close();

        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            if (line.contains("tweet_id")) {
                return;
            }
            String[] tuple = line.split(",");

            try {

                String tweet_id = tuple[0];
                String tweet_text = tuple[10];

                String[] splits = tweet_text.toString().split(" ");

                int sentiment_sum = 0;

                for (String word : splits) {

                    if (AFINN_map.containsKey(word)) {

                        Integer x = new Integer(AFINN_map.get(word));

                        sentiment_sum += x;

                    }

                    context.write(new Text(tweet_id), new Text(tweet_text + "\t----->\t" + new Text(Integer.toString(sentiment_sum))));

                }

            } catch (Exception e) {

                e.printStackTrace();

            }

        }

    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {

            context.write(key, value);

        }

    }

    public static void main(String[] args) throws Exception {

        ToolRunner.run(new Sentiment_Analysis(), args);
    }

    @Override

    public int run(String[] args) throws Exception {

// TODO Auto-generated method stub
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Sentiment Analysis");
        job.setJarByClass(Sentiment_Analysis.class);
        
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        DistributedCache.addCacheFile(new URI(args[2]), job.getConfiguration());

        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        return 0;

    }

}
