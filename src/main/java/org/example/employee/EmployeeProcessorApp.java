package org.example.employee;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
//import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.Stores;
import org.example.employee.models.Employee;
import org.example.employee.models.serdes.EmployeeDeserializer;
import org.example.employee.models.serdes.GenericSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class EmployeeProcessorApp {
    private static final String INPUT_TOPIC_NAME = "streams-employee-topic";
    private static final String STATE_STORE_NAME = "employeeStore";
    public static final String SOURCE_NAME = "EmployeeSource";
    public static final String PROCESSOR_NAME = "EmployeeProcessor";

    public static void main(String[] args) {
        final EmployeeProcessorApp app = new EmployeeProcessorApp();
        final Properties props = getKafkaConnectionConfig();
        try (final KafkaStreams streams = new KafkaStreams(app.createTopology(), props)) {

            streams.setUncaughtExceptionHandler((Throwable throwable) -> {
                System.err.println("oh no! error! " + throwable.getMessage());
                throwable.printStackTrace(System.err);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            });
            final CountDownLatch latch = new CountDownLatch(1);

            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    streams.close();
                    latch.countDown();
                }
            });

            try {
                streams.start();
                latch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }

    private Topology createTopology() {
        Topology topology = new Topology();
        topology.addSource(Topology.AutoOffsetReset.EARLIEST, SOURCE_NAME, new StringDeserializer(), new EmployeeDeserializer(), INPUT_TOPIC_NAME)
                .addProcessor(PROCESSOR_NAME, EmployeeProcessor::new, SOURCE_NAME)
                .addStateStore(
                        Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(STATE_STORE_NAME),
                                Serdes.String(),
                                Serdes.serdeFrom(
                                        new GenericSerializer<>(), new EmployeeDeserializer()
                                )
                        ),
                        PROCESSOR_NAME
                );
        return topology;
    }

    private static Properties getKafkaConnectionConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "employee-streams-pocessor-demo");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-example-employee-processor");
        return props;
    }


    private static class EmployeeProcessor implements Processor<String, Employee, String, Employee> {

        private ProcessorContext<String, Employee> context;
//        private StateStore stateStore;

        @Override
        public void init(ProcessorContext<String, Employee> context) {
            this.context = context;
//            this.stateStore = context.getStateStore(STATE_STORE_NAME);
        }

        @Override
        public void process(Record<String, Employee> record) {
            System.out.printf("\nKey = %s; Value = %s\n", record.key(), record.value());
            this.context.forward(record);
        }
    }
}
