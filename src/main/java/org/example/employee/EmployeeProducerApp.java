package org.example.employee;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.example.employee.models.Employee;
import org.example.employee.models.serdes.EmployeeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Date;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;

public class EmployeeProducerApp {
	private static final String TOPIC_NAME = "streams-employee-topic";
//	private static final String RECORD_KEY = "employee";
	private static final Logger LOGGER = LoggerFactory.getLogger("employee_producer");
	private static final FastDateFormat DATE_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd");


	public static void main(String[] args) {
		final Properties props = getKafkaConnectionConfig();
		try (Producer<String, Employee> kafkaProducer = new KafkaProducer<>(props, new StringSerializer(), new EmployeeSerializer())) {
			EmployeeProducerApp producer = new EmployeeProducerApp();
			do {
				Employee emp = producer.askForEmployee();
				kafkaProducer.send(new ProducerRecord<>(TOPIC_NAME, emp.getEmpId(), emp));
			} while(!askForBoolean("Do you want to continue (y/n)?", "y", "n", false));
		}
	}

	private Employee askForEmployee() {
		Employee emp = new Employee();
		emp.setEmpId(askForUUID());
		emp.setFirstName(askForFirstName());
		emp.setLastName(askForLastName());
		emp.setPosition(askForPosition());
		emp.setBirthDate(askForBirthDate());
		return emp;
	}

	private String askForUUID() {
		return UUID.randomUUID().toString();
	}

	private String askForFirstName() {
		return askForValue("First Name", v -> v, StringUtils::isNotBlank);
	}

	private String askForLastName() {
		return askForValue("Last Name", v -> v, StringUtils::isNotBlank);
	}

	private String askForPosition() {
		return askForValue("Position", v -> v, StringUtils::isNotBlank);
	}

	private Date askForBirthDate() {
		return askForValue("Birth Date", EmployeeProducerApp::parseDate, Objects::nonNull);
	}

	private static <T> T askForValue(String fieldName, Function<String, T> valueTransformer, Predicate<T> valueValidator) {
		T value;
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

		try {
			do {
				System.out.printf("\n%s: ", fieldName);
				value = valueTransformer.apply(reader.readLine());
			} while (!valueValidator.test(value));
			return value;
		} catch (IOException e) {
			LOGGER.error("An IO error occurred", e);
			throw new RuntimeException(e);
		}
	}

	private static Date parseDate(String dateStr) {
		try {
			return DATE_FORMAT.parse(dateStr);
		} catch (ParseException e) {
			LOGGER.error("Error parsing value: " + dateStr, e);
			return null;
		}
	}

	@SuppressWarnings("SameParameterValue")
	private static boolean askForBoolean(String prompt, String trueAnswer, String falseAnswer, boolean trueByDefault) {
		final String questionPrompt = String.format("%1$s (%2$s/%3$s) [%4$s]", prompt, trueAnswer, falseAnswer, trueByDefault ? trueAnswer : falseAnswer);
		return askForValue(questionPrompt, v -> !equalsIgnoreCase(trueAnswer, defaultIfBlank(v, trueByDefault ? trueAnswer : falseAnswer)), v -> true);
	}

	private static Properties getKafkaConnectionConfig() {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "employee-streams-demo");
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
//		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.example.employee.models.serdes.EmployeeSerializer");
		return props;
	}
}
