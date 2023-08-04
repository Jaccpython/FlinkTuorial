package com.atguigu.chaoter05Review;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformMapTestReview {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<EventReview> stream3 = env.fromElements(
                new EventReview("Mary", "./home", 1000L),
                new EventReview("Bob", "./cart", 2000L),
                new EventReview("Alice", "./prod?id=100", 3000L)
        );

        SingleOutputStreamOperator<String> map1 = stream3.map(new MyMapperReview());
        SingleOutputStreamOperator<String> map2 = stream3.map(
                (eventReview) -> {
                    return eventReview.user;
                }
        );


    }

    private static class MyMapperReview implements MapFunction<EventReview, String> {
        @Override
        public String map(EventReview eventReview) throws Exception {
            return eventReview.user;
        }
    }
}
