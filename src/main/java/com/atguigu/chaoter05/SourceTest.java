package com.atguigu.chaoter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Properties;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        // ����ִ�л���
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ���ò��ж�Ϊ1
        env.setParallelism(2);

        // 1. ���ļ��ж�ȡ����
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

        // 2. �Ӽ����ж�ȡ����
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> stream2 = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("BOb", "./cart", 2000L));
        DataStreamSource<Event> stream3 = env.fromCollection(events);

        // 3. ��Ԫ���ж�ȡԪ��
        DataStreamSource<Event> stream4 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("BOb", "./cart", 2000L)
        );

        // 4. ��socket�ı�����ȡԪ��
        // DataStreamSource<String> stream5 = env.socketTextStream("hadoop102", 7777);

        // 5. ��Kafka�ж�ȡ����
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.10.102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> kafkaString = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

//        stream1.print("1");
//        stream2.print("2");
//        stream3.print("3");
//        stream4.print("4");
//        stream5.print("5");
        kafkaString.print();

        env.execute();
    }
}
