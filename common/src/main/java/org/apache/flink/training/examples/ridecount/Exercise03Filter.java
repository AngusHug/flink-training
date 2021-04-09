package org.apache.flink.training.examples.ridecount;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.GeoUtils;

/**
 * Example:get stream where taxi in NYC
 *
 * Modify:class GeoUtils extends Serializable
 * Flink requires all dataTypes be serializable.
 * Flink supported Data Types: java Tuples ,Java POJOS, Primitive Types, Regular Classes, Values, Hadoop Writables, Special Types
 * POJOs requirements:
 *      1.class must be public
 *      2.must have a public constructor without arguments
 *      3.All fields are either public or must be accessible through getter and setter functions
 *      type of a field must bu supported by a registered serializer
 */
public class Exercise03Filter {
    public static void main(String [] args) throws Exception {
        GeoUtils geo = new GeoUtils();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaxiRide> taxi = env.addSource(new TaxiRideGenerator());

        DataStream<TaxiRide> taxi_NYC = taxi.filter(new FilterFunction<TaxiRide>() {
            @Override
            public boolean filter(TaxiRide value) throws Exception {
//                return !(value.startLon > -73.7 || value.startLon < -74.05) &&
//                        !(value.startLat > 41.0 || value.startLat < 40.5);
                return geo.isInNYC(value.startLon, value.startLat);
            }
        });
        taxi_NYC.print();
        env.execute("Filter");
    }
}
