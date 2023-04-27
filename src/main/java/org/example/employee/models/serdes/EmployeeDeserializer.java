package org.example.employee.models.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.example.employee.models.Employee;

import java.io.IOException;
import java.util.Arrays;

@Log4j
public class EmployeeDeserializer implements Deserializer<Employee> {
	private final ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public Employee deserialize(String topic, byte[] bytes) {
		try {
			log.info(String.format("Going to deserialize a topic: %s\n", topic));
			final Employee employee = objectMapper.readValue(bytes, Employee.class);
			log.info(String.format("The value readen from topic is:\n%1$s;\n bytes: %2$s", employee, Arrays.toString(bytes)));
			return employee;
		} catch (IOException e) {
			log.error("An error occurred while attempt to deserialize an Employee", e);
			throw new RuntimeException(e);
		}
	}
}
