package com.atguigu.chaoter07;

import com.atguigu.chaoter05Review.ClickSource;
import com.atguigu.chaoter05Review.EventReview;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class ProcessingTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<EventReview> stream = env.addSource(new ClickSource());

        stream.keyBy(x->x.user)
                .process(
                        new KeyedProcessFunction<String, EventReview, String>() {
                            @Override
                            public void processElement(EventReview eventReview, Context context, Collector<String> collector) throws Exception {
                                // 当前处理时间
                                Long currTs = context.timerService().currentProcessingTime();
                                collector.collect(context.getCurrentKey() + "数据到达，到达时间: " + new Timestamp(currTs));

                                // 注册一个10秒后的定时器
                                context.timerService().registerProcessingTimeTimer(currTs + 10000);
                            }

                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                out.collect(ctx.getCurrentKey() + "定时器触发，触发时间: " + new Timestamp(timestamp));
                            }
                        }
                ).print();

        env.execute();
    }
}
