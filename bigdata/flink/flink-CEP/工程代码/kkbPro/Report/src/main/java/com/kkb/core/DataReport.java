package com.kkb.core;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.kkb.function.MySumFuction;
import com.kkb.watermark.MyWaterMark;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * ETL:对数据进行预处理
 * 报表：就是要计算一些指标
 */
public class DataReport {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //env.setStateBackend(new RocksDBStateBackend(""));
        //设置time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String topic="data2";
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers","node01:9092,node02:9092,node03:9092");
        consumerProperties.put("group.id","auditLog_consumer");
        consumerProperties.put("enable.auto.commit", "false");
        consumerProperties.put("auto.offset.reset","earliest");

        //读取Kafka里面对数据
        //{"dt":"2019-11-24 21:19:47","type":"child_unshelf","username":"shenhe1","area":"AREA_ID"}
        FlinkKafkaConsumer011<String> consumer =
                new FlinkKafkaConsumer011<String>(topic,new SimpleStringSchema(),consumerProperties);


        DataStreamSource<String> data = env.addSource(consumer);

        Logger logger= LoggerFactory.getLogger(DataReport.class);

        //对数据进行处理
        SingleOutputStreamOperator<Tuple3<Long, String, String>> preData = data.map(new MapFunction<String, Tuple3<Long, String, String>>() {
            /**
             * Long:time
             * String: type
             * String: area
             * @return
             * @throws Exception
             */
            @Override
            public Tuple3<Long, String, String> map(String line) throws Exception {
                JSONObject jsonObject = JSON.parseObject(line);
                String dt = jsonObject.getString("dt");
                //按照这两个字段进行分组
                String type = jsonObject.getString("type");
                String area = jsonObject.getString("area");
                long time = 0;
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    time = sdf.parse(dt).getTime();
                } catch (Exception e){
                    logger.error("时间解析失败，dt:" + dt, e.getCause());
                }
                return Tuple3.of(time, type, area);
            }
        });

        /**
         *过滤无效的数据
         */
        SingleOutputStreamOperator<Tuple3<Long, String, String>> filterData = preData.filter(tuple3 -> tuple3.f0 != 0);
        /**
         * 收集迟到太久的数据
         */
        OutputTag<Tuple3<Long,String,String>> outputTag=
                new OutputTag<Tuple3<Long,String,String>>("late-date"){};
        /**
         * 进行窗口的统计操作
         * 统计的过去的一分钟的每个大区的,不同类型的有效视频数量
         */

        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> resultData = filterData.assignTimestampsAndWatermarks(new MyWaterMark())
                //type   area
                .keyBy(1, 2)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .sideOutputLateData(outputTag)
                .apply(new MySumFuction());

        //HBase


        /**
         * 收集到延迟太多的数据，业务里面要求写到Kafka
         */

        SingleOutputStreamOperator<String> sideOutput =
                //java8
                resultData.getSideOutput(outputTag).map(line -> line.toString());

//        String outputTopic="lateData";
//        Properties producerProperties = new Properties();
//        producerProperties.put("bootstrap.servers","node01:9092");
//        FlinkKafkaProducer011<String> producer = new FlinkKafkaProducer011<>(outputTopic,
//                new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),
//                producerProperties);
//        sideOutput.addSink(producer);

        /**
         * 业务里面需要吧数据写到ES里面
         * 而我们公司是需要把数据写到kafka
         */

        resultData.print();


        env.execute("DataReport");

    }
}
