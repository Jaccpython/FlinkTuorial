package com.atguigu.chaoter06;

import com.atguigu.chaoter05Review.ClickSource;
import com.atguigu.chaoter05Review.EventReview;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class UrlCountViewExample {
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

        stream3.print("input");

        // 统计每个url的访问量
        stream3.keyBy(x->x.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new AggregateFunction<EventReview, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(EventReview eventReview, Long aLong) {
                                return aLong + 1L;
                            }

                            @Override
                            public Long getResult(Long aLong) {
                                return aLong;
                            }

                            @Override
                            public Long merge(Long aLong, Long acc1) {
                                return aLong + acc1;
                            }
                        }, new ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>() {

                            @Override
                            public void process(String url, Context context, Iterable<Long> iterable, Collector<UrlViewCount> collector) throws Exception {
                                collector.collect(new UrlViewCount(
                                        url,
                                        iterable.iterator().next(),
                                        context.window().getStart(),
                                        context.window().getEnd()));
                            }
                        }
                ).print();

        env.execute();
    }
}
