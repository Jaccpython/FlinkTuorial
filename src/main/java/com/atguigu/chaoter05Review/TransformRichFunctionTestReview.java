package com.atguigu.chaoter05Review;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformRichFunctionTestReview {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<EventReview> stream3 = env.fromElements(
                new EventReview("Mary", "./home", 1000L),
                new EventReview("Bob", "./cart", 2000L),
                new EventReview("Alice", "./prod?id=100", 3000L)
        );

        stream3.map(new MyRichMapper()).setParallelism(2)
                .print();
        env.execute();
    }

    private static class MyRichMapper extends RichMapFunction<EventReview, Integer> {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open生命周期被调用" +
                    getRuntimeContext().getIndexOfThisSubtask() +
                    "号任务启动");
        }

        @Override
        public Integer map(EventReview eventReview) throws Exception {
            return eventReview.url.length();
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close生命周期被调用" +
                    getRuntimeContext().getIndexOfThisSubtask() +
                    "号任务启动");
        }
    }
}
