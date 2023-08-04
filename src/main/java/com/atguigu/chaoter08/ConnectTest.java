package com.atguigu.chaoter08;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> Stream1 = env.fromElements(1, 2, 3);
        DataStreamSource<Long> Stream2 = env.fromElements(1L, 2L, 3L);

        ConnectedStreams<Integer, Long> streams = Stream1.connect(Stream2);

        streams.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer integer) {
                return integer.toString();
            }

            @Override
            public String map2(Long aLong) {
                return aLong.toString();
            }
        }).print();

        env.execute();
    }
}
