package org.apache.flink.training.examples.ridecount;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;

import javax.swing.text.JTextComponent;
import javax.xml.crypto.Data;

/*
*   MapFunction<T, O):
*       <T>:input data type
*       <O>:return type
*   Exerciese01: counts driver num for every area(lon,lat) when state is start
 */
public class Exercise01Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaxiRide> source = env.addSource(new TaxiRideGenerator());

        DataStream<TaxiRide> is_start = source.filter(new FilterFunction<TaxiRide>() {
            @Override
            public boolean filter(TaxiRide taxi) throws Exception {
                return taxi.isStart;
            }
        });

        DataStream<Tuple3<Float, Float, Integer>> area = is_start.map(new MapFunction<TaxiRide, Tuple3<Float, Float, Integer>>() {
            @Override
            public Tuple3<Float, Float, Integer> map(TaxiRide value) throws Exception {
                return Tuple3.of(value.startLat, value.startLon, 1);
            }
        });

        /*
        // keySelector to define partition self
        KeyedStream<Tuple3<Float, Float, Integer>, Tuple2> keyByArea = area.keyBy(new KeySelector<Tuple3<Float, Float, Integer>, Tuple2>() {
            @Override
            public Tuple2 getKey(Tuple3<Float, Float, Integer> value) throws Exception {
                return Tuple2.of(value.f0, value.f2);
            }
        });
        */
        KeyedStream<Tuple3<Float, Float, Integer>, Tuple> keyByArea = area.keyBy(0,1);
        DataStream<Tuple3<Float, Float, Integer>> counts = keyByArea.sum(2);

        counts.print();
        env.execute();
    }
}
