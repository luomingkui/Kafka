package com.atguigu.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor<String,String> {

    private int successCount = 0;
    private int failedCount = 0;

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

        if(exception == null){
            successCount++;
        }else {
            failedCount++;
        }

    }

    public void close() {
        System.out.println("successCount:"+successCount);
        System.out.println("failedCount:"+failedCount);

    }

    public void configure(Map<String, ?> configs) {

    }
}
