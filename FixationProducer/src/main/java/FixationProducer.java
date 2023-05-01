import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;


public class FixationProducer {

    private final static String TOPIC_NAME = "fixations";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) throws Exception {

        // Read Kafka properties file and create Kafka producer with the given properties
        KafkaProducer<byte[], String> producer;
        try (InputStream props = Resources.getResource("producer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            // Create Kafka producer
            producer = new  KafkaProducer<byte[], String>(properties);
        }

        // generate random fixation events
        while (true) {


            JSONObject fixation =  generateRandomFixation();
            byte[] key = null;
            String value = fixation.toJSONString();
            System.out.println("Producing - "+value);
            ProducerRecord<byte[], String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
            producer.send(record);

            Thread.sleep(getRandomNumber(60, 1500));
        }

    }

    /*
    Generate a random nunber
    */
    private static int getRandomNumber(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }


    /*
       Generate a random fixation
       */
    public static JSONObject generateRandomFixation() {
        Random rand = new Random();

        long timestamp = System.currentTimeMillis();

        // Generate random x and y positions for the fixation point
        int xpos = rand.nextInt(1920);
        int ypos =  rand.nextInt(1080);

        // generate fixation duration between 40 and 600 ms
        double fixationDuration = rand.nextDouble() * 540.0 + 40.0;

        // Generate a random fixation dispersion value between 0 and 1
        double fixationDispersion = rand.nextDouble();

        // Generate pupil size within a small range and add/subtract a small random amount for variation
        double pupilSize = (rand.nextDouble() * 0.3) + 3.0 + ((Math.random() * 0.05) - 0.025);

        // Set event source to be always 1
        int eventSource = 1;

        // Set participant to refer always to the same participant
        String participant = "P10";

        // Set task to refer always to the same task
        String task = "Task12";

        // Create a JSONObject to store the generated fixation data
        JSONObject fixation = new JSONObject();

        // Add the generated values to the JSONObject
        fixation.put("Timestamp", timestamp);
        fixation.put("Xpos", xpos);
        fixation.put("Ypos", ypos);
        fixation.put("PupilSize", pupilSize);
        fixation.put("FixationDuration", fixationDuration);
        fixation.put("FixationDispersion", fixationDispersion);
        fixation.put("EventSource", eventSource);
        fixation.put("Participant", participant);
        fixation.put("Task", task);

        return fixation;
    }
}