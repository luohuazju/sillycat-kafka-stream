package com.sillycat.kafkastream.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickEventProduceApp {

	public static void main(String[] args) {
		log.info("Start the Kafka connection----------");

		Properties props = new Properties();
		// must configuration
		props.put("bootstrap.servers", "centos7-master:9092,centos7-worker1:9092,centos7-worker2:9092");
		// key and value, we can use string, AVRO and etc
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// options
		props.put(ProducerConfig.ACKS_CONFIG, "-1"); // 0 - ignore if writes success, -1 - performance low, but writes
														// on all, 1 between
		props.put("retries", "0"); // retry may affect duplicated messages
		props.put("batch.size", "323840"); // 32 MB, buffer batch size
		props.put("linger.ms", "1000"); // time we buffer in sending
		props.put("buffer.memory", "33554432");
		props.put("max.block.ms", "3000");
		// compression.type GZIP、Snappy、LZ4 or Zstandard
		// request.timeout.ms

		Producer<String, String> producer = new KafkaProducer<>(props);

		for (int i = 0; i < 10; i++) {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("general-topic1",
					Integer.toString(i), "content" + Integer.toString(i));
			producer.send(record); // fire and forget
			producer.send(record, new Callback() { // call back to deal with result
				@Override
				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					if (e == null) {
						log.info("send event to kafka success----");
					} else {
						log.info("send event to kafka failed-----");
						log.error(e.getMessage());
					}
				}
			});
			try {
				RecordMetadata data = (RecordMetadata) producer.send(record).get(); // block and wait the result
				log.info("successfully send event to kafka " + data.toString());
			} catch (InterruptedException e) {
				log.error(e.getMessage());
				e.printStackTrace();
			} catch (ExecutionException e) {
				log.error(e.getMessage());
				e.printStackTrace();
			}
		}
		producer.close();

	}

}
