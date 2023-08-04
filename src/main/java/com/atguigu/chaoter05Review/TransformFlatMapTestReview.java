package com.atguigu.chaoter05Review;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformFlatMapTestReview {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<EventReview> stream3 = env.fromElements(
                new EventReview("Mary", "./home", 1000L),
                new EventReview("Bob", "./cart", 2000L),
                new EventReview("Alice", "./prod?id=100", 3000L)
        );

        stream3.flatMap(
                (EventReview eventReview, Collector<String> collector) -> {
                    collector.collect(eventReview.user);
                    collector.collect(eventReview.url);
                    collector.collect(eventReview.timestamp.toString());
                }
        ).returns(Types.STRING).print();

        env.execute();
    }
}
