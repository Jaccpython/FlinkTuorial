package com.atguigu.chaoter07;

import com.atguigu.chaoter05Review.ClickSource;
import com.atguigu.chaoter05Review.EventReview;
import com.atguigu.chaoter06.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class TopNExample {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<EventReview> stream = env.addSource(new ClickSource())
                // ����
                .assignTimestampsAndWatermarks(WatermarkStrategy.<EventReview>forBoundedOutOfOrderness(Duration.ZERO).
                        withTimestampAssigner(new SerializableTimestampAssigner<EventReview>() {
                            // ˮλ��
                            @Override
                            public long extractTimestamp(EventReview eventReview, long l) {
                                return eventReview.timestamp;
                            }
                        }));

        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(x -> x.url)
                // ��������
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(
                        new AggregateFunction<EventReview, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(EventReview eventReview, Long aLong) {
                                return aLong + 1L;
                            }

                            @Override
                            public Long getResult(Long aLong) {
                                return aLong;
                            }

                            @Override
                            public Long merge(Long aLong, Long acc1) {
                                return aLong + acc1;
                            }
                        }, new ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>() {

                            @Override
                            public void process(String url, Context context, Iterable<Long> iterable, Collector<UrlViewCount> collector) throws Exception {
                                collector.collect(new UrlViewCount(
                                        url,
                                        iterable.iterator().next(),
                                        context.window().getStart(),
                                        context.window().getEnd()));
                            }
                        }
                );

        urlCountStream.keyBy(x->x.windowEnd)
                .process(new TopNProcessResult(2))
                .print();

        urlCountStream.print("url count");


        env.execute();

    }

    private static class TopNProcessResult extends KeyedProcessFunction<Long, UrlViewCount, String> {

        private Integer n;
        // �����б�״̬
        private ListState<UrlViewCount> urlViewCountListState;

        public TopNProcessResult(Integer n) {
            this.n = n;
        }

        // �ڻ����ֻ�ȡ״̬
        @Override
        public void open(Configuration parameters) throws Exception {
            urlViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>("url-count-list", Types.POJO(UrlViewCount.class))
            );
        }

        // �������� ������ ���
        @Override
        public void processElement(UrlViewCount urlViewCount, Context context, Collector<String> collector) throws Exception {
            // �����ݱ�����״̬��
            urlViewCountListState.add(urlViewCount);

            // ע��windowEnd + 1ms �Ķ�ʱ��
            context.timerService().registerEventTimeTimer(context.getCurrentKey() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();
            for (UrlViewCount urlViewCount: urlViewCountListState.get()) {
                urlViewCountArrayList.add(urlViewCount);
            }

            urlViewCountArrayList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });

            // ��װ��Ϣ ��ӡ���
            StringBuilder result = new StringBuilder();
            result.append("--------------------\n");
                result.append("���ڽ�����Ϣ: ").append(new Timestamp(ctx.getCurrentKey())).append("\n");

            for (int i = 0; i < n; i++) {
                UrlViewCount currTuple = urlViewCountArrayList.get(i);
                String info = "No. " + (i + 1) + " "
                        + "user: " + currTuple.url + " "
                        + "������: " + currTuple.count + "\n";
                result.append(info);
            }
            result.append("--------------------\n");

            out.collect(result.toString());

        }
    }
}
