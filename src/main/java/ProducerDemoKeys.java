import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final String bootstrapServers = "127.0.0.1:9092";

        //Create the producer config

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Create producer record
        for (int i = 1; i <= 10; i++) {

            final String topicName = "first_topic";
            final String value = "hello world!! " + Integer.valueOf(i);
            final String key = "id_" + Integer.valueOf(i);

            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);

            logger.info("Key :: " + key); //Logs the key

            /**
             * id_1 goes to partition 0
             * id_2 goes to partition 2
             * id_3 goes to partition 0
             * id_4 goes to partition 2
             * id_5 goes to partition 2
             * id_6 goes to partition 0
             * id_7 goes to partition 2
             * id_8 goes to partition 1
             * id_9 goes to partition 2
             * id_10 goes to partition 2
             */
            //Send data -asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //Executes every time a record is successfully sent or when exception is thrown
                    if (e == null) {
                        //if the record is successfully sent
                        logger.info("Received new metadata successfully.\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); //.get() blocks the .send() to make it synchronous - don't do this in production, This is just to verify that the keys goes to same partitions for particular topic
        }

        //flush and close the producer
        producer.flush();
        producer.close();
    }
}
