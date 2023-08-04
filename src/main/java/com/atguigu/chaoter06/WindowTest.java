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
                         * ����
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

                // ����or����

                // ���䣿��ο���

                /*

                .countWindow(
                        // ������������ ÿʮ����ͳ��һ�� ÿ����������һ��
                        // ��һ���������ǹ������� �������������ǻ�������
                        10, 2
                )


                .window(
                        // �¼��¼��Ự���� �Ự��ʱ�¼�����
                        EventTimeSessionWindows.withGap(Time.seconds(2)) // ����
                )
                .window(
                        // �����¼�ʱ�䴰�� ����ʱ�䳤�� ƫ�������� ����ʱ��ƫ�Ƽ����ӣ�ʱ���λСʱ����������˸�Сʱ��Ϊ�˾���ʱ����Ҫ-8
                        SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)) // һСʱ ƫ�������
                )

                */

                .window(
                        // �����¼�ʱ�䴰�� ����ʱ�䳤��
                        TumblingEventTimeWindows.of(Time.seconds(10)) // һСʱ
                )


                // ���ţ����ڿ���֮��Ĵ��ں��� �����RDD ÿ�鴰��һ�μ���

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
