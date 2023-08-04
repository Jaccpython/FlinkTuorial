package com.atguigu.chaoter06;

import com.atguigu.chaoter05Review.EventReview;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(200);

        DataStream<EventReview> stream3 = env.fromElements(
                new EventReview("Mary", "./home", 1000L),
                new EventReview("Bob", "./cart", 2000L),
                new EventReview("Alice", "./prod?id=100", 3000L),
                new EventReview("Bob", "./prod?id=1", 3300L),
                new EventReview("Bob", "./home", 3500L),
                new EventReview("Alice", "./prod?id=200", 3200L),
                new EventReview("Bob", "./prod?id=2", 3600L),
                new EventReview("Bob", "./prod?id=3", 4200L)
        )
//                /*
//                * Flink自带的有序流的watermark
//                * WatermarkStrategy.<>forMonotonousTimestamps()
//                * */
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<EventReview>forMonotonousTimestamps()
//                .withTimestampAssigner(new SerializableTimestampAssigner<EventReview>() {
//                    @Override
//                    public long extractTimestamp(EventReview eventReview, long l) {
//                        /*
//                        * 毫秒
//                        * 如果timestamp是秒需要乘以1000
//                        * */
//                        return eventReview.timestamp;
//                    }
//                }));

                /*
                * Flink自带的乱序流watermark
                * WatermarkStrategy.<>forBoundedOutOfOrderness(需要一个迟到时间 例Duration.ofSeconds(2) 两秒)
                * */
        .assignTimestampsAndWatermarks(WatermarkStrategy
                .<EventReview>forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner<EventReview>() {
            @Override
            public long extractTimestamp(EventReview eventReview, long l) {
                /*
                * 毫秒
                * */
                return eventReview.timestamp;
            }
        }));

        stream3.print();

        env.execute();
    }
}
