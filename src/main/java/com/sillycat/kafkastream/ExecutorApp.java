package com.sillycat.kafkastream;

import org.springframework.util.StringUtils;

import com.sillycat.kafkastream.consumer.ClickEventConsumeApp;
import com.sillycat.kafkastream.producer.ClickEventProduceApp;
import com.sillycat.kafkastream.stream.EventStreamPersistLocalFile;

import lombok.extern.slf4j.Slf4j;

/**
 *  
 * run cosumer java -jar target/kafkastream-app-1.0.0-jar-with-dependencies.jar EVENTS_CONSUMER
 * @author carl
 *
 */
@Slf4j
public class ExecutorApp {

	public static void main(String[] args) {
		log.info("Enter Executor Application--------------");

		String module = "EVENTS_PERSISTLOCAL";
		String sysModule = System.getProperty("running_module");
		log.info("get the module from ENV = " + sysModule);
		
		if (StringUtils.hasText(sysModule)) {
			module = sysModule;
		} else {
			if (args.length > 0) {
				module = args[0];
				log.info("get the module from args = " + module);
			}
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
			ClickEventConsumeApp.main(args);
			break;
		default:
			log.warn("Unknown module is in parameters module = " + module);
			break;
		}
	}

}
