/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_BloomFilter;

/**
 *
 * @author deepali
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;
import org.apache.hadoop.filecache.DistributedCache;

public class BloomFilterUsingDistributedCache {

    public static class BloomFilterMapper extends Mapper<Object, Text, Text, NullWritable> {

        Funnel<DestinationsCustom> p = new Funnel<DestinationsCustom>() {

            public void funnel(DestinationsCustom t, Sink into) {

                into.putString(t.destination, Charsets.UTF_8);

            }
        };

        private BloomFilter<DestinationsCustom> dest = BloomFilter.create(p, 500, 0.01);

        @Override
        public void setup(Context context) throws IOException, InterruptedException {

            String destination;

            try {
                Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if (files != null && files.length > 0) {

                    for (Path file : files) {

                        try {
                            File myFile = new File(file.toUri());
                            BufferedReader bufferedReader = new BufferedReader(new FileReader(myFile.toString()));
                            String line = null;
                            while ((line = bufferedReader.readLine()) != null) {
                                String[] split = line.split(" ");

                                destination = String.valueOf(split[0]);

                                DestinationsCustom d = new DestinationsCustom(destination);
                                dest.put(d);
                            }
                        } catch (IOException ex) {
                            System.err.println("Exception while reading  file: " + ex.getMessage());
                        }
                    }
                }
            } catch (IOException ex) {
                System.err.println("Exception in mapper setup: " + ex.getMessage());
            }

        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String values[] = value.toString().split(",");
            DestinationsCustom d = new DestinationsCustom(values[17]);
            if (dest.mightContain(d)) {
                context.write(value, NullWritable.get());
            }

        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Bloom Filter");
        job.setJarByClass(BloomFilterUsingDistributedCache.class);
        job.setMapperClass(BloomFilterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
       // DistributedCache.addCacheFile(new URI(args[2]), job.getConfiguration());
        DistributedCache.addCacheFile(new URI("/cache.txt"),job.getConfiguration());
        //DistributedCache.addCacheFile(new URI("file:/home/apoorva/Desktop/Deepali/ADBMS_PROJECT/cache"), job.getConfiguration());
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
