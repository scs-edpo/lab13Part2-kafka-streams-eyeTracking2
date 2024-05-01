import com.google.common.io.Resources;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class FixationConsumer {

    public static void main(String[] args) throws IOException {

            // Read Kafka properties file and create Kafka consumer with the given properties
            KafkaConsumer<String, GenericRecord> consumer;
            try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
                Properties properties = new Properties();
                properties.load(props);
                consumer = new KafkaConsumer<>(properties);
            }

            // subscribe to relevant topics
            consumer.subscribe(Arrays.asList("fixations-out"));


            while (true) {

                // pool new data
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(8));

                // process consumer records
                for (ConsumerRecord<String, GenericRecord> record : records) {

                            GenericRecord value = record.value();
                            String cl = record.partition()==1? "High": "Low";
                            System.out.println("Received fixation event: " + value + "- Cognitive load: " + cl);


                    }
                }


            }
        }



