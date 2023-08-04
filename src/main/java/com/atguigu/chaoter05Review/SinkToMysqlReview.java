package com.atguigu.chaoter05Review;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkToMysqlReview {
    public static void main(String[] args) throws Exception {
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

        stream3.addSink(JdbcSink.sink(
                "insert into clicks (user, url) values (? , ?)",
                ((preparedStatement, eventReview) -> {
                    preparedStatement.setString(1, eventReview.user);
                    preparedStatement.setString(2, eventReview.url);
                }),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://hadoop102:3306/test")
                .withDriverName("com.mysql.jdbc.Driver")
                .withUsername("root")
                .withPassword("000000")
                .build()
        ));

        env.execute();

    }
}
