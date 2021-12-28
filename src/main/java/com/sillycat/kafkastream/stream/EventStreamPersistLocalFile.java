package com.sillycat.kafkastream.stream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventStreamPersistLocalFile {


	public static void main(String[] args) {
		log.info("Start the Kafka connection----------");
		Properties prop = new Properties();
		prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "Events2LocalFile");
		prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
				"centos7-master:9092,centos7-worker1:9092,centos7-worker2:9092");
		prop.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 3000);
		prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<Object, Object> source = builder.stream("events_local_file");

		source.foreach((x, y) -> {
			log.info("x: " + x + "  y: " + y);
		});

		final Topology topo = builder.build();
		final KafkaStreams streams = new KafkaStreams(topo, prop);

		final CountDownLatch latch = new CountDownLatch(1);
		Runtime.getRuntime().addShutdownHook(new Thread("stream") {
			public void run() {
				streams.close();
				latch.countDown();
			}
		});
		try {
			streams.start();
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.exit(0);

	}

}
