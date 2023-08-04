package com.atguigu.test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;
import java.util.Random;

public class test1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<Long, Long>> stream = env.addSource(new SourceFunction<Tuple2<Long, Long>>() {

            public Boolean running = true;

            @Override
            public void run(SourceContext<Tuple2<Long, Long>> sourceContext) throws Exception {
                Random random = new Random();
                while (running) {
                    sourceContext.collect(Tuple2.of(random.nextLong(), System.currentTimeMillis()));
                    Thread.sleep(1000L);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<Long, Long>>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(
                (SerializableTimestampAssigner<Tuple2<Long, Long>>) (aLong, l) -> aLong.f1
        ));

        stream.print();

        env.execute();
    }
}
