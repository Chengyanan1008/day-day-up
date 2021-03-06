package com.kkb.core;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kkb.source.KkbRedisSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

/**
 * 数据清洗
 *
 *
 * 码表
 */
public class DataClean {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //我们是从Kafka里面读取数据，所以这儿就是topic有多少个partition，那么就设置几个并行度。
        env.setParallelism(3);
        /**
         * flink整合kafka，将kafka的offset保存到了checkPoint里面去了
         */
        env.enableCheckpointing(60000);//每隔多长时间进行一次checkPoint
        //底层其实使用的是分段提交 二段提交的方式  ==》 保证exactly  once这样的一种消费方式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);//配置两次checkPoint的间隔时间
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);  //最大的checkPoint的并发数
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //注释，我们这儿其实需要设置state backed类型，我们要把checkpoint的数据存储到
        //rocksdb里面

        //第一步：从Kafka里面读取数据 消费者 数据源需要kafka
        //topic的取名还是有讲究的，最好就是让人根据这个名字就能知道里面有什么数据。
        //xxxx_xxx_xxx_xxx
        String topic="data1";
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers","node01:9092,node02:9092,node03:9092");
        consumerProperties.put("group.id","allTopic_consumer");
        consumerProperties.put("enable.auto.commit", "false");
        consumerProperties.put("auto.offset.reset","earliest");

        /**
         * String topic, 主题
         * KafkaDeserializationSchema<T> deserializer,
         * Properties props
         */
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(topic,
                new SimpleStringSchema(),
                consumerProperties);


        //{"dt":"2019-11-24 19:54:23","countryCode":"PK","data":[{"type":"s4","score":0.8,"level":"C"},{"type":"s5","score":0.2,"level":"C"}]}

        //从kafak里面获取数据了
        DataStreamSource<String> allData = env.addSource(consumer);

        //设置为广播变量  获取redis里面的数据，作为广播变量  将redids的数据做广播变量
        DataStream<HashMap<String, String>> mapData = env.addSource(new KkbRedisSource()).broadcast();

        SingleOutputStreamOperator<String> etlData = allData
                .connect(mapData)
                .flatMap(new CoFlatMapFunction<String, HashMap<String, String>, String>() {
                    //其实就是解析两个流数据  第一个流数据从kafka里面来的，第二个流数据，广播出去的redis的 数据


            HashMap<String, String> allMap = new HashMap<String, String>();
            //里面处理的是kafka的数据
            @Override
            public void flatMap1(String line, Collector<String> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(line);
                String dt = jsonObject.getString("dt");
                String countryCode = jsonObject.getString("countryCode");
                //可以根据countryCode获取大区的名字
                String area = allMap.get(countryCode);
                JSONArray data = jsonObject.getJSONArray("data");
                for (int i = 0; i < data.size(); i++) {
                    JSONObject dataObject = data.getJSONObject(i);
                    System.out.println("大区："+area);
                    //测试字体
                    dataObject.put("dt", dt);
                    dataObject.put("area", area);
                    //下游获取到数据的时候，也就是一个json格式的数据
                    out.collect(dataObject.toJSONString());
                }
            }
            //里面处理的是redis里面的数据
            @Override
            public void flatMap2(HashMap<String, String> map,
                                 Collector<String> collector) throws Exception {
                System.out.println(map.toString());
                allMap = map;
            }
        });
        //ETL -> load kafka
        etlData.print().setParallelism(1);
        /**
         * String topicId,
         * SerializationSchema<IN> serializationSchema,
         * Properties producerConfig)
         */
//        String outputTopic="allDataClean";
//        Properties producerProperties = new Properties();
//        producerProperties.put("bootstrap.servers","192.168.167.254:9092");
//        FlinkKafkaProducer011<String> producer = new FlinkKafkaProducer011<>(outputTopic,
//                new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),
//                producerProperties);
//
//        //搞一个Kafka的生产者
//        etlData.addSink(producer);
        env.execute("DataClean");
    }
}
