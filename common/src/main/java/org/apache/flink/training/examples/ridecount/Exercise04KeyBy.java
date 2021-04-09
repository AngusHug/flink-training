package org.apache.flink.training.examples.ridecount;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.util.Collector;

public class Exercise04KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaxiRide> source = env.addSource(new TaxiRideGenerator());
        DataStream<Tuple3<Long, Long, Integer>> cnt = source
                        .keyBy((TaxiRide taxi) -> taxi.driverId)
                        .window(TumblingEventTimeWindows.of(Time.seconds(20)))
                        .process(new sumPassengerCnt());
        cnt.print();
        env.execute("KeyBy");
    }
    public static class  sumPassengerCnt extends ProcessWindowFunction<TaxiRide, Tuple3<Long, Long, Integer>, Long, TimeWindow> {

        /**
         * Evaluates the window and outputs none or several elements.
         *
         * @param aLong    The key for which this window is evaluated.
         * @param context  The context in which the window is being evaluated.
         * @param elements The elements in the window being evaluated.
         * @param out      A collector for emitting elements.
         * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
         */
        @Override
        public void process(Long aLong, Context context, Iterable<TaxiRide> elements, Collector<Tuple3<Long, Long, Integer>> out) throws Exception {
            int cnt = 0;
            for(TaxiRide taxi:elements){
                cnt += taxi.passengerCnt;
            }
            out.collect(Tuple3.of(context.window().getEnd(), aLong, cnt));
        }
    }
}
