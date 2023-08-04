package com.atguigu.chaoter06;

import com.atguigu.chaoter05Review.ClickSource;
import com.atguigu.chaoter05Review.EventReview;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;

public class WindowAggregateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(200);

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

        stream3.keyBy(x -> x.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new AggregateFunction<EventReview, Tuple2<Long, Integer>, String>() {
                            /**
                             * 创建一个ACC（累加器）
                             * @return 返回一个ACC累加器
                             */
                            @Override
                            public Tuple2<Long, Integer> createAccumulator() {
                                return Tuple2.of(0L, 0);
                            }

                            /**
                             * 中间状态
                             * @param eventReview 每个数调一次
                             * @param longIntegerTuple2 这个数的状态（总和， 个数） 用来求平均值
                             * @return 放回这个数的状态
                             */
                            @Override
                            public Tuple2<Long, Integer> add(EventReview eventReview, Tuple2<Long, Integer> longIntegerTuple2) {
                                return Tuple2.of(eventReview.timestamp + longIntegerTuple2.f0, longIntegerTuple2.f1 + 1);
                            }

                            /**
                             * 每个累加器的最终状态
                             * @param longIntegerTuple2 最后的中间状态
                             * @return 由最后的中间状态变成我们需要的最终状态
                             */
                            @Override
                            public String getResult(Tuple2<Long, Integer> longIntegerTuple2) {
                                Timestamp timestamp = new Timestamp(longIntegerTuple2.f0 / longIntegerTuple2.f1);
                                return timestamp.toString();
                            }

                            /**
                             * 所有窗口的累加器的处理
                             * @param longIntegerTuple2 累加器leader
                             * @param acc1 累加器
                             * @return 最终所有累加器处理完后的累加器
                             */
                            @Override
                            public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> longIntegerTuple2, Tuple2<Long, Integer> acc1) {
                                return Tuple2.of(longIntegerTuple2.f0+acc1.f0, longIntegerTuple2.f1+acc1.f1);
                            }
                        }
                ).print();

        env.execute();

    }
}
