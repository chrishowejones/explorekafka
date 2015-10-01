package com.devcycle.explorekafka.training;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * Created by chrishowe-jones on 16/09/15.
 */
public class KafkaProducer {

    private final ProducerConfig config;
    private final Producer<String, String> producer;
    private final long events;

    public KafkaProducer(String[] args) {
        events = Long.parseLong(args[0]);
        Properties props = buildProperties(args);
        config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }

    public void emitEvents() {
        Random rnd = new Random();
        for (long nEvents = 0; nEvents < events; nEvents++) {
            System.out.println("creating event " + nEvents);
            long runtime = new Date().getTime();
            String ip = "192.168.2." + rnd.nextInt(255);
            String msg = runtime + ",www.devcycle.com," + ip;
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("devcycle123", ip, msg);
            producer.send(data);
        }
        producer.close();
    }

    private Properties buildProperties(String[] args) {
        String metadataBrokerList = "localhost:9092";
        String zookeeper = "localhost:2181";
        if (args.length > 1) {
            metadataBrokerList = args[1];
        }
        if (args.length > 2) {
            zookeeper = args[2];
        }
        Properties props = new Properties();
        props.put("metadata.broker.list", metadataBrokerList);
        props.put("zk.connect", zookeeper);
        props.put("producer.type", "sync");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "com.devcycle.explorekafka.training.KafkaPartitioner");
        props.put("request.required.acks", "1");
        return props;
    }

    public static void main(String[] args) {
        KafkaProducer kafkaProducer = new KafkaProducer(args);
        kafkaProducer.emitEvents();
    }


}
