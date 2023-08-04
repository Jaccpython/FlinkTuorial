package com.atguigu.chaoter07;

import com.atguigu.chaoter05Review.ClickSource;
import com.atguigu.chaoter05Review.EventReview;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;

public class TopNExample_ProcessAllWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<EventReview> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<EventReview>forBoundedOutOfOrderness(Duration.ZERO).
                        withTimestampAssigner((SerializableTimestampAssigner<EventReview>) (eventReview, l) -> eventReview.timestamp));

        stream.map(x -> x.user)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlHashMapCountAgg(), new UrlALLWindowResult())
                .print();

        env.execute();

    }

    // 自定义增量聚合函数
    private static class UrlHashMapCountAgg implements AggregateFunction<String, HashMap<String, Long>, ArrayList<Tuple2<String, Long>>> {
        @Override
        public HashMap<String, Long> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Long> add(String s, HashMap<String, Long> stringLongHashMap) {
            if (stringLongHashMap.containsKey(s)) {
                stringLongHashMap.put(s, stringLongHashMap.get(s) + 1);
            } else {
                stringLongHashMap.put(s, 1L);
            }
            return stringLongHashMap;
        }

        @Override
        public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> stringLongHashMap) {
            ArrayList<Tuple2<String, Long>> result = new ArrayList<>();
            for (String key : stringLongHashMap.keySet()) {
                result.add(Tuple2.of(key, stringLongHashMap.get(key)));
            }
            result.sort((o1, o2) -> o2.f1.intValue() - o1.f1.intValue());
            return result;
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> stringLongHashMap, HashMap<String, Long> acc1) {
            return null;
        }
    }

    private static class UrlALLWindowResult extends ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow> {
        @Override
        public void process(Context context, Iterable<ArrayList<Tuple2<String, Long>>> iterable, Collector<String> collector) throws Exception {
            ArrayList<Tuple2<String, Long>> next = iterable.iterator().next();

            StringBuilder result = new StringBuilder();
            result.append("--------------------\n");
            result.append("窗口结束信息: ").append(new Timestamp(context.window().getEnd())).append("\n");

            for (int i = 0; i < 2; i++) {
                Tuple2<String, Long> currTuple = next.get(i);
                String info = "No. " + (i + 1) + " "
                        + "user: " + currTuple.f0 + " "
                        + "访问量: " + currTuple.f1 + "\n";
                result.append(info);
            }
            result.append("--------------------\n");

            collector.collect(result.toString());
        }
    }
}
