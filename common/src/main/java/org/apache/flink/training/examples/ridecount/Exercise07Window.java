package org.apache.flink.training.examples.ridecount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.util.Collector;

/**
 * Example:calculate fare of each driver ,hour by hour
 * Window: defined on already partitioned KeyedStreams.KeyedStream--->WindowedStream
 *          Windows group the data in each key accroding to some characteristic.
 */
public class Exercise07Window {
    public static void main(String [] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaxiFare> source = env.addSource(new TaxiFareGenerator());

        // TumblingEventTimeWindow 滚动窗口
        DataStream<Tuple3<Long, Long, Float >> fareHour = source
                        .keyBy((TaxiFare taxi)->taxi.driverId)
                        .window(TumblingEventTimeWindows.of(Time.hours(1)))
                        .process(new sumTips())
                        ;
        // SlidingWindows
        DataStream<Tuple3<Long, Long, Float>> fareSliding = source
                            .keyBy((TaxiFare taxi) -> taxi.driverId)
                            .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(2)))
                            .process(new sumTips());

        // WindowAll
        DataStream<Tuple3<Long, Long, Float>> fareWindowAll = source
                            .keyBy((TaxiFare taxi) -> taxi.driverId)
                            .window(GlobalWindows.create())
                            .process(new ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, GlobalWindow>() {
                                @Override
                                public void process(Long aLong, Context context, Iterable<TaxiFare> elements, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
                                    float sum_tips = 0;
                                    for(TaxiFare fare:elements){
                                        sum_tips += fare.tip;
                                    }
                                    out.collect(Tuple3.of(GlobalWindow.get().maxTimestamp(), aLong, sum_tips));
                                }
                            });
        env.execute("Window");
    }
    public static class sumTips extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {


        @Override
        public void process(Long aLong, Context context, Iterable<TaxiFare> elements, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            float sum_tips = 0;
            for(TaxiFare fare:elements){
                sum_tips += fare.tip;
            }
            out.collect(Tuple3.of(context.window().getEnd(), aLong, sum_tips));
        }
    }
}
