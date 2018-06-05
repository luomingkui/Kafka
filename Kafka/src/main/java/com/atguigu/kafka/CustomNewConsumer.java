package com.atguigu.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 案例三：自动维护消费情况的API
 */
public class CustomNewConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();

        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "hadoop102:9092");
        //制定consumer group
        props.put("group.id", "test");
        // 是否自动提交offset
        props.put("enable.auto.commit", "true");
        //自动确认offset时间间隔
        props.put("auto.commit.interval.ms", "1000");
        //key的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //values的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("first", "second", "three"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("record.offset():" + record.offset() + ",record.key():" + record.key() + "record.value():" + record.value()+"\t");
        }

    }
}
