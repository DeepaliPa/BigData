package Project_RandomForest;


import Project_RandomForest.FlightClassifyReducer;
import Project_RandomForest.FlightModelMapper;
import Project_RandomForest.FlightWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Forest {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Random Forest");
        job.setJarByClass(Forest.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlightWritable.class);

        job.setMapperClass(FlightModelMapper.class);
        //job.setCombinerClass(FlightStatsReducer.class);
        job.setReducerClass(FlightClassifyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
