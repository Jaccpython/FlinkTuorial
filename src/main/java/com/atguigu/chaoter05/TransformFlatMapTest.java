package com.atguigu.chaoter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Alice", "./cart", 2000L),
                new Event("Bob", "./prod?id=100", 3000L)
        );

        // SingleOutputStreamOperator<String> alice = stream.flatMap(new MyFlatMap());

//        SingleOutputStreamOperator<String> alice = stream.flatMap( (Event event, Collector<String> collector) -> {
//            collector.collect(event.user);
//            collector.collect(event.url);
//            collector.collect(event.timestamp.toString());
//        } )
//                .returns(Types.STRING);
        SingleOutputStreamOperator<String> alice = stream.flatMap( (Event event, Collector<String> collector) -> {
            collector.collect(event.user);
            collector.collect(event.url);
            collector.collect(event.timestamp.toString());
        } )
                .returns(new TypeHint<String>() {});

        alice.print();

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<Event, String> {

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            collector.collect(event.user);
            collector.collect(event.url);
            collector.collect(event.timestamp.toString());
        }
    }
}
