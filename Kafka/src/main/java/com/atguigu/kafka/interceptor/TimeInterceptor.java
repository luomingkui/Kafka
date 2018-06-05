package com.atguigu.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 增加时间戳拦截器
 */
public class TimeInterceptor implements ProducerInterceptor<String,String> {


    /**
     * 该方法封装进KafkaProducer.send方法中，即它运行在用户主线程中。Producer确保在消息被序列化以及计算分区前调用该方法。用户可以在该方法中对消息做任何操作，但最好保证不要修改消息所属的topic和分区，否则会影响目标分区的计算
     * @param record
     * @return
     */
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return new ProducerRecord(record.topic(),record.partition(),record.timestamp(),record.key(),record.value());
    }

    /**
     * 该方法会在消息被应答或消息发送失败时调用
     * @param metadata
     * @param exception
     */
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    /**
     * 关闭interceptor，主要用于执行一些资源清理工作
     */
    public void close() {

    }

    /**
     * 获取配置信息和初始化数据时调用。
     * @param configs
     */
    public void configure(Map<String, ?> configs) {

    }
}
