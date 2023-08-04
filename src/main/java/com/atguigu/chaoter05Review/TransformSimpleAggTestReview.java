package com.atguigu.chaoter05Review;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformSimpleAggTestReview {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<EventReview> stream3 = env.fromElements(
                new EventReview("Mary", "./home", 1000L),
                new EventReview("Bob", "./cart", 2000L),
                new EventReview("Alice", "./prod?id=100", 3000L),
                new EventReview("Bob", "./prod?id=1", 3300L),
                new EventReview("Bob", "./home", 3500L),
                new EventReview("Alice", "./prod?id=200", 3200L),
                new EventReview("Bob", "./prod?id=2", 3600L),
                new EventReview("Bob", "./prod?id=3", 4200L)
        );

        stream3.keyBy(
                eventReview -> eventReview.user
        ).max("timestamp").print("max:");
        stream3.keyBy(
                eventReview -> eventReview.user
        ).maxBy("timestamp").print("maxBy:");
        env.execute();
    }
}
