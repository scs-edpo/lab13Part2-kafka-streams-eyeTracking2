import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import utils.AoiNames;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;


public class ClicksProducer {

    private final static String TOPIC_NAME = "clicks";
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

        // generate random click events
        while (true) {


            JSONObject click =  generateRandomClick();
            byte[] key = null;
            String value = click.toJSONString();
            System.out.println("Producing - "+value);
            ProducerRecord<byte[], String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
            producer.send(record);

            Thread.sleep(getRandomNumber(100, 2000));
        }

    }

    /*
    Generate a random nunber
    */
    private static int getRandomNumber(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }

    public static JSONObject generateRandomClick() {
        Random rand = new Random();

        long timestamp = System.currentTimeMillis();

        // Generate random x and y positions for click points
        int x = rand.nextInt(1920);
        int y = rand.nextInt(1080);

        // Array of AOI names
        String[] aoiNames = {
                "BestSellers",
                "NewArrivals",
                "TrendingCollections",
                "CustomerReviews",
                "SpecialOffers"
        };

        // Get a random AOI value from the AoiName enum
        String aoi = AoiNames.values()[rand.nextInt(AoiNames.values().length)].toString();


        // Create a JSONObject to store the generated click data
        JSONObject click = new JSONObject();

        // Add the generated values to the JSONObject
        click.put("Timestamp", timestamp);
        click.put("X", x);
        click.put("Y", y);
        click.put("aoi", aoi);

        return click;
    }
}