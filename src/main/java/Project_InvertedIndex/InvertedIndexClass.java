/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_InvertedIndex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author deepali
 */
public class InvertedIndexClass {

    public static class InvertedMapper extends Mapper<Object, Text, CustomWritableInvertedInd, Text> {

        public void map(Object key, Text values, Context context) {
            try {
                
                String val = values.toString();
                
                if (val.contains("UniqueCarrier")) {
                    return;
                }
                
                String[] result = val.split(",");
                String flightNumber = result[9].trim();
                String origin = result[16].trim();
                String destination = result[17].trim();

                if (origin.equals(" ")) {

                    origin = "NA";
                }
                if (destination.equals(" ")) {

                    destination = "NA";
                }
                if (flightNumber.equals("NA")) {

                    flightNumber = "0";
                }
                if (flightNumber.equals(" ")) {

                    flightNumber = "0";
                }

                CustomWritableInvertedInd customWritableInvertedInd = new CustomWritableInvertedInd(origin, destination);

                context.write(customWritableInvertedInd, new Text(flightNumber));

            } catch (Exception ex) {

                ex.printStackTrace();

            }

        }
    }

    public static class CustomWritableInvertedInd implements Writable, WritableComparable<CustomWritableInvertedInd> {

        String origin;
        String destination;

        public CustomWritableInvertedInd() {
        }

        public CustomWritableInvertedInd(String origin, String destination) {
            this.origin = origin;
            this.destination = destination;
        }

        public void write(DataOutput dataOutput) throws IOException {
            WritableUtils.writeString(dataOutput, origin);
            WritableUtils.writeString(dataOutput, destination);

        }

        public void readFields(DataInput dataInput) throws IOException {
            origin = WritableUtils.readString(dataInput);
            destination = WritableUtils.readString(dataInput);
        }

        public String getOrigin() {
            return origin;
        }

        public void setOrigin(String origin) {
            this.origin = origin;
        }

        public String getDestination() {
            return destination;
        }

        public void setDestination(String destination) {
            this.destination = destination;
        }

        public int compareTo(CustomWritableInvertedInd o) {
            int result = origin.compareTo(o.origin);
            if (0 == result) {
                result = destination.compareTo(o.destination);
            }
            return result;
        }

        @Override
        public String toString() {
            return (new StringBuilder().append(origin).append("\t").append("to").append("\t").append(destination).append("\t")).toString();
        }

    }

    public static class SecondarySortBasicCompKeySortComparator extends WritableComparator {

        protected SecondarySortBasicCompKeySortComparator() {
            super(CustomWritableInvertedInd.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            CustomWritableInvertedInd key1 = (CustomWritableInvertedInd) w1;
            CustomWritableInvertedInd key2 = (CustomWritableInvertedInd) w2;

            return key1.getOrigin().compareTo(key2.getOrigin());
        }
    }

    public static class InvertedReducer extends Reducer<CustomWritableInvertedInd, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(CustomWritableInvertedInd key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();
            boolean first = true;

            for (Text t : values) {
                if (!sb.toString().contains(t.toString())) {
                    if (first) {
                        first = false;
                    } else {
                        sb.append("   ");
                    }
                    sb.append(t.toString());
                }
            }
            result.set(sb.toString());
            context.write(new Text(key.toString()), result);
        }

    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Inverted Index");
            job
                    .setJarByClass(InvertedIndexClass.class
                    );

            job
                    .setMapperClass(InvertedMapper.class
                    );
            job
                    .setMapOutputKeyClass(CustomWritableInvertedInd.class
                    );
            job
                    .setMapOutputValueClass(Text.class
                    );
            job
                    .setGroupingComparatorClass(SecondarySortBasicCompKeySortComparator.class
                    );
            job
                    .setReducerClass(InvertedReducer.class
                    );
            job
                    .setOutputKeyClass(Text.class
                    );
            job
                    .setOutputValueClass(Text.class
                    );
            FileInputFormat.addInputPath(job, new Path(args[0]));
            TextOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
        } catch (Exception ex) {

            ex.printStackTrace();

        }
    }

}
