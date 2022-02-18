package edu.uci.ics.tippers.config;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

// import com.fasterxml.jackson.databind.deser.std.StringDeserializer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import edu.uci.ics.tippers.model.middleware.mget_obj;
import edu.uci.ics.tippers.model.middleware.LeTime;
@EnableKafka
@Configuration
public class KafkaConfig {
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "group_id");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        

        return new DefaultKafkaConsumerFactory<String, String>(configs);
    }

    @Bean
    public ConsumerFactory<String, mget_obj> mgetobjConsumerFactory(){
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "group_json");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        return new DefaultKafkaConsumerFactory<String, mget_obj>(configs, new StringDeserializer(), new JsonDeserializer<>(mget_obj.class));
    }
    @Bean
    public ConsumerFactory<String, LeTime> leTimeConsumerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "group_json");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        return new DefaultKafkaConsumerFactory<String, LeTime>(configs, new StringDeserializer(), new JsonDeserializer<>(LeTime.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(){
        ConcurrentKafkaListenerContainerFactory<String, String>  factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, mget_obj> mget_objKakfaListenerFactory(){
        ConcurrentKafkaListenerContainerFactory<String, mget_obj>  factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(mgetobjConsumerFactory());
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, LeTime> leTimeKafkaListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, LeTime> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(leTimeConsumerFactory());
        return factory;
    }

}
