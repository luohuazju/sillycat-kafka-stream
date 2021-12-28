package com.sillycat.kafkastream.stream;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class WindowStream {

	public static void main(String[] args) {
		log.info("Start to init the kafka connection----------");
		Properties prop = new Properties();
		prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "WindowStream");
		prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
				"centos7-master:9092,centos7-worker1:9092,centos7-worker2:9092");
		prop.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 3000);
		prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<Object, Object> source = builder.stream("topicA");
		KTable<Windowed<String>, Long> countKtable = source
				.flatMapValues(value -> Arrays.asList(value.toString().split("s+"))).map((x, y) -> {
					return new KeyValue<String, String>(y, "1");
				}).groupByKey().windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5))).count();

		countKtable.toStream().foreach((x, y) -> {
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
