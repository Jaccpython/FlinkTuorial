package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class StreamWorCount {
    public static void main(String[] args) throws Exception {
        // 1. ������ʽִ�л���
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // �Ӳ�������ȡ�������Ͷ˿ں�
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String hostname = parameterTool.get("host");
//        Integer port = parameterTool.getInt("port");

        // 2. ��ȡ�ı���
        DataStreamSource<String> linDataStream = env.socketTextStream("hadoop102", 7777);

        // 3. ת������
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = linDataStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. ����
        KeyedStream<Tuple2<String, Long>, String> wordAneOneKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);

        // 5. ���
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAneOneKeyedStream.sum(1);

        // 6. ��ӡ
        sum.print();

        // 7. ����ִ��
        env.execute();

    }
}
