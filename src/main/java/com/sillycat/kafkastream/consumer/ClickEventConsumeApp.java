package com.sillycat.kafkastream.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickEventConsumeApp {

	public static void main(String[] args) {
		log.info("Start the Kafka connection----------");

		String topicName = "general-topic1";
		String groupID = "group_consumers_app";
		Properties props = new Properties();
		props.put("bootstrap.servers", "centos7-master:9092,centos7-worker1:9092,centos7-worker2:9092");
		props.put("group.id", groupID);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		props.put("enable.auto.commit", "true"); //
		props.put("auto.commit.interval.ms", "1000"); //
		props.put("auto.offset.reset", "earliest"); // earliest, latest, none
		//session.timeout.ms let group know one consumer is down
		//max.poll.interval.ms max time we process the logic
		//fetch.max.bytes size of message we can fetch each time
		//max.poll.records messages we fetch, default is 500 count

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		consumer.subscribe(Arrays.asList(topicName));

		try {
			while (true) { // always running
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(60));
				for (ConsumerRecord<String, String> record : records) {
					log.info("offset = %d, value = %s", record.offset(), record.value());
				}
			}
		} finally {
			consumer.close();
		}

	}

}
