package com.atguigu.chaoter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformReduceTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Alice", "./cart", 2000L),
                new Event("Bob", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home1", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Bob", "./prod?id=3", 4200L)
        );

        stream.map((MapFunction<Event, Tuple2<String, Long>>) event -> Tuple2.of(event.user, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG)).keyBy(x -> x.f0)
                .reduce((ReduceFunction<Tuple2<String, Long>>) (stringLongTuple2, t1) -> Tuple2.of(stringLongTuple2.f0, stringLongTuple2.f1 + t1.f1)).keyBy(x -> "key" )
                .reduce((ReduceFunction<Tuple2<String, Long>>) (stringLongTuple2, t1) -> stringLongTuple2.f1 > t1.f1 ? stringLongTuple2 : t1).print();

        env.execute();
    }
}
