package com.sillycat.kafkastream.model;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@EqualsAndHashCode
@AllArgsConstructor
@Data
public class ClickEvent {

	private String name;
	private Integer age;
	private String action;
	private String content;
	private Date eventTime;

}
