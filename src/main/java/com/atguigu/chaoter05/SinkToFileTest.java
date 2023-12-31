package com.atguigu.chaoter05;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class SinkToFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Alice", "./cart", 2000L),
                new Event("Bob", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home1", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Bob", "./prod?id=3", 4200L)
        );

        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"),
                new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                                //默认滚动策略
                        DefaultRollingPolicy.builder()
                                // 文件多大
                        .withMaxPartSize(1024 * 1024 * 1024)
                                // 时间多长
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                // 非活跃多久
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                        .build()
                )
                .build();

        stream.map(Event::toString).addSink(streamingFileSink);
        env.execute();

    }
}
