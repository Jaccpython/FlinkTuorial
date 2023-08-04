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

// ����ͳ��pv��uv������������õ�ƽ���û���Ծ��
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
                                 * ����
                                 * */
                                return eventReview.timestamp;
                            }
                        }));


        stream3.print("data");

        // �������ݷ���һ��ͳ��pv��uv
        stream3.keyBy(x -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new AvgPv()).print();

        env.execute();

    }

    // �Զ���һ��AggregateFunction����Long����pv��������HashSetȥ��uvȥ��
    public static class AvgPv implements AggregateFunction<EventReview, Tuple2<Long, HashSet<String>>, Double> {

        /**
         * ��ʼ���ۼ���
         * @return �ۼ���
         */
        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L, new HashSet<>());
        }

        /**
         * ÿ��һ�����ݣ������ۼ�����״̬
         * @param eventReview ������¼�����
         * @param longHashSetTuple2 ��ǰ���ۼ���״̬
         * @return ���º���ۼ���״̬
         */
        @Override
        public Tuple2<Long, HashSet<String>> add(EventReview eventReview, Tuple2<Long, HashSet<String>> longHashSetTuple2) {
            // ÿ��һ�����ݣ�pv������1����user����HashSet��
            longHashSetTuple2.f1.add(eventReview.user);
            return Tuple2.of(longHashSetTuple2.f0+1, longHashSetTuple2.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> longHashSetTuple2) {
            // ���ڳ���ʱ�����pv��uv�ı�ֵ
            return (double) longHashSetTuple2.f0 / longHashSetTuple2.f1.size();
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
            return null;
        }
    }

}
