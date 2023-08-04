package com.atguigu.chaoter06;


import com.atguigu.chaoter05Review.ClickSource;
import com.atguigu.chaoter05Review.EventReview;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

// 开窗统计pv和uv，两者相除，得到平均用户活跃度
public class WindowAggregateTest_PvUV {
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


        stream3.print("data");

        // 所有数据放在一起统计pv和uv
        stream3.keyBy(x -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new AvgPv()).print();

        env.execute();

    }

    // 自定义一个AggregateFunction，用Long保存pv个数，用HashSet去做uv去重
    public static class AvgPv implements AggregateFunction<EventReview, Tuple2<Long, HashSet<String>>, Double> {

        /**
         * 初始化累加器
         * @return 累加器
         */
        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L, new HashSet<>());
        }

        /**
         * 每来一条数据，更新累加器的状态
         * @param eventReview 输入的事件数据
         * @param longHashSetTuple2 当前的累加器状态
         * @return 更新后的累加器状态
         */
        @Override
        public Tuple2<Long, HashSet<String>> add(EventReview eventReview, Tuple2<Long, HashSet<String>> longHashSetTuple2) {
            // 每来一条数据，pv个数加1，将user放入HashSet中
            longHashSetTuple2.f1.add(eventReview.user);
            return Tuple2.of(longHashSetTuple2.f0+1, longHashSetTuple2.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> longHashSetTuple2) {
            // 窗口出发时，输出pv和uv的比值
            return (double) longHashSetTuple2.f0 / longHashSetTuple2.f1.size();
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
            return null;
        }
    }

}
