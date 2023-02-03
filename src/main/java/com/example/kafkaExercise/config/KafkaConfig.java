package com.example.kafkaExercise.config;

import com.example.kafkaExercise.util.PurchaseLogOneProductSerializer;
import com.example.kafkaExercise.util.PurchaseLogSerializer;
import com.example.kafkaExercise.util.WatchingAdLogSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
@EnableKafka
public class KafkaConfig {

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration myKStreamConfig() {
        Map<String, Object> myKStreamConfig = new HashMap<>();
        myKStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "lecture-6");
        myKStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "13.125.205.11:9092, 3.36.63.75:9092, 54.180.1.108:9092");
        myKStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        myKStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        myKStreamConfig.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
        myKStreamConfig.put(StreamsConfig.topicPrefix(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG), 2);
        myKStreamConfig.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        return new KafkaStreamsConfiguration(myKStreamConfig);
    }


    @Bean
    public KafkaTemplate<String, Object> KafkaTemplateForGeneral() {
        return new KafkaTemplate<String, Object>(ProducerFactory());
    }

    @Bean
    public ProducerFactory<String, Object> ProducerFactory() {
        Map<String, Object> myConfig = new HashMap<>();

        myConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "13.125.205.11:9092, 3.36.63.75:9092, 54.180.1.108:9092");
        myConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        myConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PurchaseLogOneProductSerializer.class);

        return new DefaultKafkaProducerFactory<>(myConfig);
    }

    @Bean
    public KafkaTemplate<String, Object> KafkaTemplateForWatchingAdLog() {
        return new KafkaTemplate<String, Object>(ProducerFactoryForWatchingAdLog());
    }
    @Bean
    public ProducerFactory<String, Object> ProducerFactoryForWatchingAdLog() {
        Map<String, Object> myConfig = new HashMap<>();

        myConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "13.125.205.11:9092, 3.36.63.75:9092, 54.180.1.108:9092");
        myConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        myConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, WatchingAdLogSerializer.class);

        return new DefaultKafkaProducerFactory<>(myConfig);
    }

    @Bean
    public KafkaTemplate<String, Object> KafkaTemplateForPurchaseLog() {
        return new KafkaTemplate<String, Object>(ProducerFactoryForPurchaseLog());
    }
    @Bean
    public ProducerFactory<String, Object> ProducerFactoryForPurchaseLog() {
        Map<String, Object> myConfig = new HashMap<>();

        myConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "13.125.205.11:9092, 3.36.63.75:9092, 54.180.1.108:9092");
        myConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        myConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PurchaseLogSerializer.class);

        return new DefaultKafkaProducerFactory<>(myConfig);
    }
//
//    @Bean
//    public ConsumerFactory<String, Object> ConsumerFactory() {
//        Map<String, Object> myConfig = new HashMap<>();
//        myConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "13.125.129.151:9092, 3.39.236.110:9092, 13.125.110.158:9092");
//        myConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        myConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        return new DefaultKafkaConsumerFactory<>(myConfig);
//    }
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, Object> myfactory = new ConcurrentKafkaListenerContainerFactory<>();
//        myfactory.setConsumerFactory(ConsumerFactory());
//        return myfactory;
//    }

}
