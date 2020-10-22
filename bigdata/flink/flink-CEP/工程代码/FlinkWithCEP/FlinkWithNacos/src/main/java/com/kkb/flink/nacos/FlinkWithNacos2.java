package com.kkb.flink.nacos;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * 实现动态的更改flink当中的checkpoint的配置
 * checkpPoint里面的数据不让更改的
 *
 */
public class FlinkWithNacos2 {
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String serverAddr = "node02";
        String dataId = "test";
        String group = "DEFAULT_GROUP";
        Properties properties = new Properties();
        properties.put("serverAddr", serverAddr);
        ConfigService configService = NacosFactory.createConfigService(properties);
        final String[] content = {configService.getConfig(dataId, group, 5000)};
        configService.addListener(dataId, group, new Listener() {
            @Override
            public Executor getExecutor() {
                return null;
            }

            @Override
            public void receiveConfigInfo(String configInfo) {
                System.out.println("===============");
                content[0] = configInfo;
                env.getCheckpointConfig().setCheckpointInterval(Long.valueOf(content[0]));
                System.out.println("----------");
                System.out.println(env.getCheckpointConfig().getCheckpointInterval());
            }
        });
        //设置checkpoint的频率间隔
        env.getCheckpointConfig().setCheckpointInterval(Long.valueOf(content[0]));
        env.getCheckpointConfig().setCheckpointTimeout(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        env.addSource(new SourceFunction<Tuple2<String, Long>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                while (true) {
                    ctx.collect(new Tuple2<>("gebi_laowang", System.currentTimeMillis()));
                    Thread.sleep(800);
                }
            }
            @Override
            public void cancel() {
            }
        }).print();
        env.execute();
    }

}
