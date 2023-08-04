package com.atguigu.wcReview;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWorCount2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> hadoop102 = env.socketTextStream("hadoop102", 44444);

        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = hadoop102.flatMap(
                (String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }
        )
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        KeyedStream<Tuple2<String, Long>, String> wordByKeyIsGroup = wordAndOne.keyBy(data -> data.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordByKeyIsGroup.sum(1);
        sum.print();
        env.execute();
    }
}


