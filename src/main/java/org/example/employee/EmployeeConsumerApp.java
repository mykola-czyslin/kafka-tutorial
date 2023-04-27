package org.example.employee;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.example.employee.models.Employee;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmployeeConsumerApp {
	private static final String TOPIC_NAME = "streams-employee-topic";
	private static final int GIVE_UP_COUNT = 100;
	private static final Duration POLL_DURATION = Duration.ofSeconds(5);

	public static void main(String[] args) {
		try (KafkaConsumer<String, Employee> consumer = new KafkaConsumer<>(getKafkaConnectionConfig())) {
			consumer.subscribe(Collections.singletonList(TOPIC_NAME));
			for (int denyCount = 0; denyCount < GIVE_UP_COUNT; ) {
				ConsumerRecords<String, Employee> consumerRecords = consumer.poll(POLL_DURATION);

				if (consumerRecords.count() == 0) {
					denyCount++;
					continue;
				}

				consumerRecords.forEach(cr -> System.out.printf("Consumed record: (%1$s, %2$s, %3$d, %4$d)\n", cr.key(), cr.value(), cr.partition(), cr.offset()));
				consumer.commitAsync();
			}
		}
	}

	private static Properties getKafkaConnectionConfig() {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "employee-streams-demo");
		props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-example-consumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.example.employee.models.serdes.EmployeeDeserializer");
		return props;
	}


}
