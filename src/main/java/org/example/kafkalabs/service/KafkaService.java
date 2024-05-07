package org.example.kafkalabs.service;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.Time;
import org.example.kafkalabs.config.kafka.KafkaTopicConfig;
import org.example.kafkalabs.utill.KafkaConnectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

@Service
public class KafkaService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaConnectMapper kafkaConnectMapper;

    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaService.class);

    public KafkaService(KafkaTemplate<String, String> kafkaTemplate,
                                         KafkaConnectMapper kafkaConnectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaConnectMapper = kafkaConnectMapper;
    }

    public <T> void sendToKafka(T object, String topic) {
        String messageToSend = kafkaConnectMapper.mapObjectToStringMessage(object);
        LOGGER.warn("Trying to send {} to topic {}", messageToSend, topic);
        try (Producer<String, String> producer = kafkaTemplate.getProducerFactory().createProducer()) {
            kafkaTemplate.send(topic, messageToSend);
            //kafkaTemplate.send(KafkaTopicConfig.PRODUCER_METRICS_TOPIC, getMetricsString(producer.metrics()));
            LOGGER.warn("Successfully sent object to topic {}", topic);
        } catch (Exception e) {
            LOGGER.error("Error occurred while putting object {} to topic {}. Details {}", object, topic, e.getMessage());
        }
    }

    private String getMetricsString(Map<MetricName, ? extends Metric> metricNameMap) {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<MetricName, ? extends Metric> entry : metricNameMap.entrySet()) {
            MetricName metricName = entry.getKey();
            Metric metric = entry.getValue();
            stringBuilder.append(metricName.name())
                    .append(": ")
                    .append(metric.metricValue())
                    .append("\n");
        }
        return stringBuilder.toString();
    }
}
