package com.atguigu.chaoter06;

import com.atguigu.chaoter05Review.ClickSource;
import com.atguigu.chaoter05Review.EventReview;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class WindowProcessTest {
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

        stream3.keyBy(x -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(
                        // EventReview: ����Ԫ�ص�����
                        // String: ���Ԫ�ص�����
                        // Boolean: ���ں����ĵ�һ�����Ӳ���������
                        // TimeWindow: ���ڵ�����
                        new ProcessWindowFunction<EventReview, String, Boolean, TimeWindow>() {
                            @Override
                            public void process(Boolean aBoolean,
                                                Context context, // ������ʱ�� ���Ի�ô�����Ϣ����ǰ�Ĵ���ʱ�䣬��ǰ��ˮλ�ߣ���ǰ��״̬����output�������
                                                Iterable<EventReview> iterable, // ���루���е����ݶ�����õ������У�
                                                Collector<String> collector) throws Exception {


                                // ��һ��HashSet����user
                                HashSet<String> userSet = new HashSet<>();

                                // ��iterable�б������ݣ��ŵ�set��
                                for (EventReview eventReview : iterable) {
                                    userSet.add(eventReview.user);
                                }

                                int uv = userSet.size();

                                // ��ϴ�����Ϣ�����
                                long start = context.window().getStart();
                                long end = context.window().getEnd();

                                collector.collect("����" + new Timestamp(start) + " ~ " + new Timestamp(end)
                                + " uvֵΪ: " + uv);

                            }
                        }
                ).print();

        env.execute();
    }
}
