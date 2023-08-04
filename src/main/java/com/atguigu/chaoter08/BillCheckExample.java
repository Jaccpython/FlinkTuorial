package com.atguigu.chaoter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class BillCheckExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ƽ̨��Ϣ��
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L),
                Tuple3.of("order-3", "app", 3500L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                        return stringStringLongTuple3.f2;
                    }
                }));

        // ������ƽ̨��Ϣ��
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdPartyStream2 = env.fromElements(
                Tuple4.of("order-1", "third-party", "success", 3000L),
                Tuple4.of("order-2", "third-party", "success", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple4<String, String, String, Long> stringStringLongTuple4, long l) {
                        return stringStringLongTuple4.f3;
                    }
                }));

        appStream.connect(thirdPartyStream2)
                .keyBy(x -> x.f0, x -> x.f0)
                .process(new OrderMatchResult())
                .print();

        env.execute();
    }

    private static class OrderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {
        // ����״̬���������������Ѿ�������¼�
        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>> thirdPartyEventState;

        @Override
        public void open(Configuration parameters) throws Exception {
            appEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
            );

            thirdPartyEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple4<String, String, String, Long>>("thirdParty-event", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG))
            );

        }

        @Override
        public void processElement1(Tuple3<String, String, Long> stringStringLongTuple3, Context context, Collector<String> collector) throws Exception {
            // ������app event������һ�������¼��Ƿ�����
            if (thirdPartyEventState.value() != null) {
                collector.collect("���˳ɹ�: " + stringStringLongTuple3 + " " + thirdPartyEventState.value());
                // ���״̬
                thirdPartyEventState.clear();
            } else {
                // ����״̬
                appEventState.update(stringStringLongTuple3);
                // ע��һ����ʱ������ʼ�ȴ���һ�������¼�
                context.timerService().registerEventTimeTimer(stringStringLongTuple3.f2 + 5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> stringStringStringLongTuple4, Context context, Collector<String> collector) throws Exception {
            if (appEventState.value() != null) {
                collector.collect("���˳ɹ�: " + appEventState.value() + " " + stringStringStringLongTuple4);
                // ���״̬
                appEventState.clear();
            } else {
                // ����״̬
                thirdPartyEventState.update(stringStringStringLongTuple4);
                // ע��һ����ʱ������ʼ�ȴ���һ�������¼�
                context.timerService().registerEventTimeTimer(stringStringStringLongTuple4.f3);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // ��ʱ���������ж�״̬�����ĳ��״̬��Ϊ�գ�˵����һ�������¼�û��
            if (appEventState.value() != null) {
                out.collect("����ʧ��: " + appEventState.value() + " " + "������֧��ƽ̨��Ϣδ��");
            }
            if (thirdPartyEventState.value() != null) {
                out.collect("����ʧ��: " + thirdPartyEventState.value() + " " + "app��Ϣδ��");
            }
            appEventState.clear();
            thirdPartyEventState.clear();
        }
    }
}