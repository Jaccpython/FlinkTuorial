package com.atguigu.chaoter05Review;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class SinkToFileTestReview {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<EventReview> stream3 = env.fromElements(
                new EventReview("Mary", "./home", 1000L),
                new EventReview("Bob", "./cart", 2000L),
                new EventReview("Alice", "./prod?id=100", 3000L),
                new EventReview("Bob", "./prod?id=1", 3300L),
                new EventReview("Bob", "./home", 3500L),
                new EventReview("Alice", "./prod?id=200", 3200L),
                new EventReview("Bob", "./prod?id=2", 3600L),
                new EventReview("Bob", "./prod?id=3", 4200L)
        );

        StreamingFileSink<String> build = StreamingFileSink.<String>forRowFormat(new Path("./output"),
                new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024 * 1024) /* 多大字节回滚 */
                                .withInactivityInterval(TimeUnit.MILLISECONDS.toMillis(15)) /* 多长毫秒时间回滚 */
                                .withRolloverInterval(TimeUnit.MILLISECONDS.toMillis(5)) /* 当前文件多久没保存数据（没有数据到来）回滚 */
                                .build()
                )
                .build();

        stream3.map(EventReview::toString).addSink(build);

    }
}
