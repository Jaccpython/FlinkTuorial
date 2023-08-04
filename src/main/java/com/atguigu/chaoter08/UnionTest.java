package com.atguigu.chaoter08;

import com.atguigu.chaoter05Review.EventReview;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class UnionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<EventReview> stream1 = env.socketTextStream("hadoop102", 7777).map(
                x -> {
                    String[] split = x.split(",");
                    return new EventReview(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                }
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<EventReview>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<EventReview>) (eventReview, l) -> eventReview.timestamp));
        stream1.print("stream1");

        SingleOutputStreamOperator<EventReview> stream2 = env.socketTextStream("hadoop103", 7777).map(
                x -> {
                    String[] split = x.split(",");
                    return new EventReview(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                }
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<EventReview>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((SerializableTimestampAssigner<EventReview>) (eventReview, l) -> eventReview.timestamp));
        stream2.print("stream2");

        // 合并两条流
        stream1.union(stream2)
                .process(new ProcessFunction<EventReview, String>() {
                    @Override
                    public void processElement(EventReview eventReview, Context context, Collector<String> collector) {
                        collector.collect("水位线: " + context.timerService().currentWatermark());
                    }
                }).print();

        env.execute();
    }
}
