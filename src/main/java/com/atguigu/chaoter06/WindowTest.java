package com.atguigu.chaoter06;

import com.atguigu.chaoter05Review.ClickSource;
import com.atguigu.chaoter05Review.EventReview;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(200);

//        DataStream<EventReview> stream3 = env.fromElements(
//                new EventReview("Mary", "./home", 1000L),
//                new EventReview("Bob", "./cart", 2000L),
//                new EventReview("Alice", "./prod?id=100", 3000L),
//                new EventReview("Bob", "./prod?id=1", 3300L),
//                new EventReview("Bob", "./home", 3500L),
//                new EventReview("Alice", "./prod?id=200", 3200L),
//                new EventReview("Bob", "./prod?id=2", 3600L),
//                new EventReview("Bob", "./prod?id=3", 4200L)
//        )
//
          SingleOutputStreamOperator<EventReview> stream3 = env.addSource(new ClickSource())
                  .assignTimestampsAndWatermarks(WatermarkStrategy
                .<EventReview>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<EventReview>() {
                    @Override
                    public long extractTimestamp(EventReview eventReview, long l) {
                        /*
                         * 毫秒
                         * */
                        return eventReview.timestamp;
                    }
                }));

        stream3.map(
                new MapFunction<EventReview, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(EventReview eventReview) throws Exception {
                        return Tuple2.of(eventReview.user, 1L);
                    }
                }
        )
                .keyBy(x -> x.f0)

                // 分配or安排

                // 分配？如何开窗

                /*

                .countWindow(
                        // 滑动计数窗口 每十个数统计一次 每两个数滑动一次
                        // 填一个参数就是滚动窗口 填两个参数就是滑动窗口
                        10, 2
                )


                .window(
                        // 事件事件会话窗口 会话超时事件两秒
                        EventTimeSessionWindows.withGap(Time.seconds(2)) // 两秒
                )
                .window(
                        // 滑动事件时间窗口 滑动时间长度 偏移量？？ 整个时间偏移几分钟，时差，单位小时，东八区快八个小时，为了纠正时差需要-8
                        SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)) // 一小时 偏移五分钟
                )

                */

                .window(
                        // 滚动事件时间窗口 滚动时间长度
                        TumblingEventTimeWindows.of(Time.seconds(10)) // 一小时
                )


                // 安排？窗口开了之后的窗口函数 相对于RDD 每块窗口一次计算

                .reduce(
                        new ReduceFunction<Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                                return Tuple2.of(stringLongTuple2.f0, stringLongTuple2.f1 + t1.f1);
                            }
                        }
                ).print();


        env.execute();

    }
}
