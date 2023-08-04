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
                                 * 毫秒
                                 * */
                                return eventReview.timestamp;
                            }
                        }));

        stream3.keyBy(x -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(
                        // EventReview: 输入元素的类型
                        // String: 输出元素的类型
                        // Boolean: 窗口函数的第一个附加参数的类型
                        // TimeWindow: 窗口的类型
                        new ProcessWindowFunction<EventReview, String, Boolean, TimeWindow>() {
                            @Override
                            public void process(Boolean aBoolean,
                                                Context context, // 上下文时间 可以获得窗口信息，当前的处理时间，当前的水位线，当前的状态，和output侧输出流
                                                Iterable<EventReview> iterable, // 输入（所有的数据都放入该迭代器中）
                                                Collector<String> collector) throws Exception {


                                // 用一个HashSet保存user
                                HashSet<String> userSet = new HashSet<>();

                                // 从iterable中遍历数据，放到set中
                                for (EventReview eventReview : iterable) {
                                    userSet.add(eventReview.user);
                                }

                                int uv = userSet.size();

                                // 结合窗口信息给输出
                                long start = context.window().getStart();
                                long end = context.window().getEnd();

                                collector.collect("窗口" + new Timestamp(start) + " ~ " + new Timestamp(end)
                                + " uv值为: " + uv);

                            }
                        }
                ).print();

        env.execute();
    }
}
