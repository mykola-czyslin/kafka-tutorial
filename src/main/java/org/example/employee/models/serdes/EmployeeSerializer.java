package org.example.employee.models.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.example.employee.models.Employee;

import java.io.IOException;


public class EmployeeSerializer implements Serializer<Employee> {
	private final ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public byte[] serialize(String s, Employee employee) {
		try {
			return objectMapper.writeValueAsBytes(employee);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
