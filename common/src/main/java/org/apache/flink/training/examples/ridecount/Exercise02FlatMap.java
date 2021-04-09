package org.apache.flink.training.examples.ridecount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.util.Collector;


/**
  *  Example:convert stream to String ,then split with delimiter as output
  *
  *  FlatMap:input one element and produces zero, one or more elements.
  *  FlatMapFunction(T, O>
  *     T:type of input
  *     O:return Type
 */


public class Exercise02FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaxiRide> taxi = env.addSource(new TaxiRideGenerator());

        DataStream<String> data = taxi.flatMap(new FlatMapFunction<TaxiRide, String>() {
            @Override
            public void flatMap(TaxiRide value, Collector<String> out) throws Exception {
                String stream = value.toString();
                for(String elem:stream.split(",")){
                    out.collect(elem);
                }
                System.out.println(stream);
            }
        });
        data.print();
        env.execute("FlatMap");
    }
}
