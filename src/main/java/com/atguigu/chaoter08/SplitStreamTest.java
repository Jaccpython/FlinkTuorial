package com.atguigu.chaoter08;

import com.atguigu.chaoter05Review.ClickSource;
import com.atguigu.chaoter05Review.EventReview;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class SplitStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<EventReview> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<EventReview>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(
                        new SerializableTimestampAssigner<EventReview>() {
                            @Override
                            public long extractTimestamp(EventReview eventReview, long l) {
                                return eventReview.timestamp;
                            }
                        }
                ));

        // 定义输出标签
        OutputTag<Tuple3<String, String, Long>> maryTag = new OutputTag<Tuple3<String, String, Long>>("Mary"){};
        OutputTag<Tuple3<String, String, Long>> bobTag = new OutputTag<Tuple3<String, String, Long>>("Bob"){};


        SingleOutputStreamOperator<EventReview> processStream = stream.process(
                new ProcessFunction<EventReview, EventReview>() {
                    @Override
                    public void processElement(EventReview eventReview,
                                               Context context,
                                               Collector<EventReview> collector)
                            throws Exception {
                        if (eventReview.user.equals("Mary")) {
                            context.output(maryTag, Tuple3.of(eventReview.user, eventReview.url, eventReview.timestamp));
                        } else if (eventReview.user.equals("Bob")) {
                            context.output(bobTag, Tuple3.of(eventReview.user, eventReview.url, eventReview.timestamp));
                        } else collector.collect(eventReview);
                    }
                }
        );

        processStream.print("else");
        processStream.getSideOutput(maryTag).print("Mary");
        processStream.getSideOutput(bobTag).print("Bob");

        env.execute();
    }
}
