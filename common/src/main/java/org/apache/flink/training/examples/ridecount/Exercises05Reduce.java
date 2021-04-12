package org.apache.flink.training.examples.ridecount;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.util.Collector;
import java.time.Duration;


/**
 * Example: sum driving time for each driver
 * reduce: work on keyed data stream. conmbines the current element with the last reduced value
 *          and emits the new value. like recursion algorithms.
 */
public class Exercises05Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaxiRide> source = env.addSource(new TaxiRideGenerator());
        DataStream<Tuple2<Long, Long>> driving_time = source
                        .filter(new filter())
                        .flatMap(new flatmap());

        DataStream<Tuple2<Long, Long>> drive_time = driving_time.keyBy(0).reduce(new sumTime());

        drive_time.print();
        env.execute("DrivingTime");
    }
    public static class  filter implements FilterFunction<TaxiRide>{
        @Override
        public boolean filter(TaxiRide taxi){
            return taxi.isStart;
        }
    }
    public static class flatmap implements FlatMapFunction<TaxiRide, Tuple2<Long, Long>>{

        @Override
        public void flatMap(TaxiRide value, Collector<Tuple2<Long, Long>> out) throws Exception {
            out.collect(Tuple2.of(value.driverId,
                    Duration.between(value.endTime, value.startTime).toMillis()));
        }
    }

    public static class sumTime implements ReduceFunction<Tuple2<Long, Long>>{
        @Override
        public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) throws Exception {
            return Tuple2.of( value1.f0, value1.f1 + value2.f1);
        }
    }
}

