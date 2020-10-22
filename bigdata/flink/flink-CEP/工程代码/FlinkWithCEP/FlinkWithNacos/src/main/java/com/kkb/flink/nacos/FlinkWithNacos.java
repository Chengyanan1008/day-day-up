package com.kkb.flink.nacos;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Properties;
import java.util.concurrent.Executor;

public class FlinkWithNacos {
    public static void main(String[] args) throws Exception {
        //获取程序的入口类
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String serverAddr = "node02";  //指定nacos的主机名称
        String dataId = "test";
        String group = "DEFAULT_GROUP";
        Properties properties = new Properties();
        properties.put("serverAddr", serverAddr);
        //获取nacos服务
        ConfigService configService = NacosFactory.createConfigService(properties);

        String content = configService.getConfig(dataId, group, 5000);

        System.out.println("main " + content);

        //自定义数据源，在数据源当中进行动态的获取配置文件
        env.addSource(new RichSourceFunction<String>() {
            ConfigService configService;
            String config;
            String dataId = "test";
            String group = "DEFAULT_GROUP";

            /*
            config表示的就是我们的配置项，在open方法里面进行初始化
            注意open方法只会初始化一次
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                String serverAddr = "node02";
                Properties properties = new Properties();
                properties.put("serverAddr", serverAddr);
                configService = NacosFactory.createConfigService(properties);
                config = configService.getConfig(dataId, group, 5000);
                //给configService  添加了一个监听器  监听器主要就是监听config的配置属性的变化，一旦变化了，就更改config的值
                configService.addListener(dataId, group, new Listener() {
                    @Override
                    public Executor getExecutor() {
                        return null;
                    }

                    @Override
                    public void receiveConfigInfo(String configInfo) {
                        config = configInfo;
                        System.out.println("open Listener receive : " + configInfo);
                    }
                });
            }

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
//                config = configService.getConfig(dataId, group, 5000);
                while (true) {
                    System.out.println("run config = " + config);
                    Thread.sleep(Integer.parseInt(config));//程序会休眠5000毫秒值
                    ctx.collect(String.valueOf(System.currentTimeMillis()));
                }
            }

            @Override
            public void cancel() {

            }
        }).print();


        env.execute("laowang");
    }
}