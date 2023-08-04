package com.atguigu.chaoter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformMapTest {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Alice", "./cart", 2000L),
                new Event("Bob", "./prod?id=100", 3000L)
        );

        // SingleOutputStreamOperator<String> result = stream.map(new MyMapper());

        SingleOutputStreamOperator<Event> filter = stream.filter(x -> x.user.equals("Alice"));
        // SingleOutputStreamOperator<Event> filter = stream.filter(new MyFilter());

        filter.print();

        env.execute();

    }

    public static class MyFilter implements FilterFunction<Event> {

        @Override
        public boolean filter(Event event) throws Exception {
            return !event.user.equals("Alice");
        }
    }
}
