package com.example.myinstance;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class MyInstanceApplication implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(MyInstanceApplication.class);

    private static String replyTopic = "replyTopic";
    private static String requestTopic = "test";

    private static ReplyingKafkaTemplate<String, String, String> requestReplyKafkaTemplate;

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-instance-group");

        return props;
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    @Bean
    public ProducerFactory requestProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public ConsumerFactory consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public KafkaMessageListenerContainer<String, String> replyListenerContainer() {
        ContainerProperties containerProperties = new ContainerProperties(replyTopic);
        return new KafkaMessageListenerContainer<>(consumerFactory(), containerProperties);
    }

    @Bean
    public ReplyingKafkaTemplate<String, String, String> replyKafkaTemplate(ProducerFactory<String, String> pf,
                                                                            KafkaMessageListenerContainer<String, String> lc) {
        return new ReplyingKafkaTemplate<>(pf, lc);
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ApplicationContext context = SpringApplication.run(MyInstanceApplication.class, args);
        requestReplyKafkaTemplate = context.getBean(ReplyingKafkaTemplate.class);

        Random random = new Random();

        int id = random.nextInt();

        for(int i =0 ; i < 100; i++) {

            Thread.sleep(1000);

            ProducerRecord<String, String> record = new ProducerRecord<>(requestTopic, "Запрос " + id);
            // set reply topic in header
            record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, replyTopic.getBytes()));
            // post in kafka topic
            RequestReplyFuture<String, String, String> sendAndReceive = requestReplyKafkaTemplate.sendAndReceive(record);

            System.out.println(sendAndReceive.get().value());
        }

        //SendResult<String,String> result = sendAndReceive.getSendFuture().get();

        //result.getProducerRecord().headers().forEach(header -> System.out.println(header.key() + ":" + header.value().toString()));

    }

    @Override
    public void run(String...args) throws Exception {
        logger.info("Application started with command-line arguments: {} . \n To kill this application, press Ctrl + C.", Arrays.toString(args));
    }

}
