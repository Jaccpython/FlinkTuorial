package com.atguigu.chaoter07;

import com.atguigu.chaoter05Review.EventReview;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class EventTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<EventReview> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <EventReview>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<EventReview>) (eventReview, l) -> eventReview.timestamp
                        ));

        stream.keyBy(x->x.user)
                .process(
                        new KeyedProcessFunction<String, EventReview, String>() {
                            @Override
                            public void processElement(EventReview eventReview, Context context, Collector<String> collector) {
                                // ��ǰ����ʱ��
                                Long currTs = context.timestamp();
                                collector.collect(context.getCurrentKey()
                                        + "���ݵ��ʱ���: " + new Timestamp(currTs)
                                        + "watermark: " + context.timerService().currentWatermark());

                                // ע��һ��10���Ķ�ʱ��
                                context.timerService().registerEventTimeTimer(currTs + 10000);
                            }

                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
                                out.collect(ctx.getCurrentKey()
                                        + "��ʱ������������ʱ��: " + new Timestamp(timestamp)
                                        + "watermark: " + ctx.timerService().currentWatermark());
                            }
                        }
                ).print();

        env.execute();
    }

    public static class CustomSource implements SourceFunction<EventReview> {

        @Override
        public void run(SourceContext<EventReview> sourceContext) throws Exception {
            sourceContext.collect(new EventReview("Mary", "./home", 1000L));

            Thread.sleep(5000L);

            sourceContext.collect(new EventReview("Alice", "./home", 1100L));

            Thread.sleep(5000L);

        }

        @Override
        public void cancel() {

        }
    }

}
