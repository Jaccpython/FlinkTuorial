package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1.������Ⱥ����
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.���ļ��ж�ȡ����
        DataSource<String> lineDataSource = env.readTextFile("input/words.txt");

        // 3.��ÿ�����ݽ��зִʣ�ת���ɶ�Ԫ������
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            // ��һ���ı����зִ�
            String[] words = line.split(" ");
            // ��ÿ������ת���ɶ�Ԫ�����
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4.����word���з���
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);

        // 5.�����ڽ��оۺ�ͳ��
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

        sum.print();

    }
}
