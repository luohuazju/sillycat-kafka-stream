package com.sillycat.kafkastream;

import org.springframework.util.StringUtils;

import com.sillycat.kafkastream.producer.ClickEventProduceApp;
import com.sillycat.kafkastream.stream.EventStreamPersistLocalFile;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExecutorApp {

	public static void main(String[] args) {
		log.info("Enter Executor Application--------------");

		String module = "EVENTS_PERSISTLOCAL";
		String sysModule = System.getProperty("running_module");
		if (StringUtils.hasText(sysModule)) {
			module = sysModule;
		}

		switch (module) {
		case "EVENTS_PERSISTLOCAL":
			log.info("streaming on topic events persist local");
			EventStreamPersistLocalFile.main(args);
			break;
		case "EVENTS_PRODUCE":
			log.info("events produce to topic");
			ClickEventProduceApp.main(args);
			break;
		case "EVENTS_CONSUMER":
			log.info("events consumer to topic");
			ClickEventProduceApp.main(args);
			break;
		default:
			log.warn("Unknown module is in parameters module = " + module);
			break;
		}
	}

}
