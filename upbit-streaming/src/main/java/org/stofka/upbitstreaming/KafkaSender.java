package org.stofka.upbitstreaming;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class KafkaSender {
    private final Producer<String, String> producer;

    public KafkaSender(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
    }

    public void send(String dataType, String message) {
        String topic = getTopicForType(dataType); // 데이터 유형에 맞는 토픽 선택

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        producer.send(record, (RecordMetadata metadata, Exception e) -> {
            if (e != null) {
                e.printStackTrace();
            } else {
            }
        });
    }

    private String getTopicForType(String dataType) {
        switch (dataType) {
            case "ticker":
                return "market_ticker";
            case "trade":
                return "market_trade";
            case "orderbook":
                return "market_orderbook";
            case "candle":
                return "market_candle";
            default:
                throw new IllegalArgumentException("❌ 지원되지 않는 데이터 유형: " + dataType);
        }
    }
}