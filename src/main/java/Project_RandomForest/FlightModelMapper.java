package Project_RandomForest;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author deepali
 */


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightModelMapper extends Mapper<Object, Text, Text, FlightWritable>{

	private Text mapKey = new Text();
	private FlightWritable flight=new FlightWritable();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		flight.set(value.toString());

		if(flight.getIsSane() && !flight.getCancelled()) {
	
			mapKey.set(flight.getCarrier());
			context.write(mapKey, flight);
			
			mapKey.set(flight.getDestination());
			context.write(mapKey, flight);
			
			mapKey.set(flight.getOrigin());
			context.write(mapKey, flight);

		}
	}
}
