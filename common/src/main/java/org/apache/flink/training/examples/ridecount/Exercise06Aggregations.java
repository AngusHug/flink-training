package org.apache.flink.training.examples.ridecount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Example: get min/max amount deposit hpf
 *
 */
public class Exercise06Aggregations {
    public static void main(String [] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> source = env.readTextFile(
                "D:\\flink-training\\common\\src\\main\\java\\org\\apache\\flink\\training\\exercises\\common\\sources\\Table"
                        , "gbk");
        DataStream<Tuple3<String, String, Float>> stream = source.flatMap(new Split());
        DataStream<Tuple3<String, String, Float>> min = stream.keyBy(1).min(2);
        DataStream<Tuple3<String, String, Float>> minBy = stream.keyBy(1).minBy(2);
        DataStream<Tuple3<String, String, Float>> sum = stream.keyBy(1).sum(2);
//        min.print();
//        minBy.print();
        sum.print();
//        sumBy.print();
        env.execute("MinOrMax");
    }

    public static class Split implements FlatMapFunction<String, Tuple3<String, String, Float>>{

        public void flatMap(String value, Collector out) throws Exception {
            String [] elem = value.split("\t");
            out.collect(Tuple3.of(elem[0], elem[1], Float.parseFloat(elem[2])));
            }
        }
}
