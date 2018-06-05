package com.atguigu.kafka.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class Application {

    public static void main(String[] args) {

        //定义输入的topic
        String from = "first";
        String to = "second";

        //设置参数
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG,"logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        StreamsConfig config = new StreamsConfig(settings);

        //创建拓图
        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("SOURCE", from)
                .addProcessor("PROCESS", new ProcessorSupplier<byte[], byte[]>() {
                    public Processor<byte[], byte[]> get() {
                        // 具体分析处理
                        return new LogProcessor();
                    }
                }, "SOURCE")
                .addSink("SINK", to, "PROCESS");
        //创建kafka stream
        KafkaStreams  stream = new KafkaStreams(builder,config);
        stream.start();


    }

}
