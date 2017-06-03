/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Project_CancellationPerCarrier;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author apoorva
 */
public class CancellationMapper  extends Mapper<LongWritable, Text, Text, IntWritable>{
        
        private final static IntWritable one = new IntWritable(1);
        Text carrier = new Text();

        
        public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {

            String headervalue = value.toString();
                       
            if(key.get()==0 && headervalue.contains("UniqueCarrier")){
            return;
            }   
            
            String[]  result= headervalue.split(",");            
            

            
            if(result[21].trim().equals("1")) {
                       
            carrier.set(result[8]);
    
            context.write(carrier, one);
            
            }
        }
        
    }
    

