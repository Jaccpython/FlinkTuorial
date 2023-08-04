package com.atguigu.chaoter07;

import com.atguigu.chaoter05Review.ClickSource;
import com.atguigu.chaoter05Review.EventReview;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<EventReview> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <EventReview>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<EventReview>) (eventReview, l) -> eventReview.timestamp
                        ));

        stream.process(
                new ProcessFunction<EventReview, String>() {
                    @Override
                    public void processElement(EventReview eventReview, Context context, Collector<String> collector) throws Exception {
                        if (eventReview.user.equals("Mary")) {
                            collector.collect(eventReview.user + " clicks " + eventReview.url);
                        } else if (eventReview.user.equals("Bob")) {
                            collector.collect(eventReview.user);
                            collector.collect(eventReview.user);
                        }
                        collector.collect(eventReview.toString());
                        System.out.println("timestamp" + context.timestamp());
                        System.out.println("watermark" + context.timerService().currentWatermark());

                        System.out.println(getRuntimeContext().getIndexOfThisSubtask());
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                    }
                }
        ).print();

        env.execute();
    }
}
