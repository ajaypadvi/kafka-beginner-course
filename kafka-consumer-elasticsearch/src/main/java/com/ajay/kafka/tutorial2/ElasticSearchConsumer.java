package com.ajay.kafka.tutorial2;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private final static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    private final static String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private final static String TOPIC_NAME = "twitter_topic";
    private final static String CONSUMER_GROUP_ID = "kafka-demo-elasticsearch";


    public static void main(String[] args) throws IOException {
        ElasticSearchConsumer elasticSearchConsumer = new ElasticSearchConsumer();
        RestHighLevelClient client = elasticSearchConsumer.createElasticClient();

//        JsonObject json = new JsonObject();
//        json.addProperty("foo", "bar");
//        String jsonAsString = json.toString();

//        IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(jsonAsString, XContentType.JSON);
//        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

//        String id = indexResponse.getId();
//        logger.info("Id for data indexed in elastic index {} is {}", "twitter", id);

        //Create kafka Consumer

        KafkaConsumer<String, String> kafkaConsumer = elasticSearchConsumer.createKafkaConsumer(TOPIC_NAME);

        // poll for new data
        while (true) {
            ConsumerRecords<String, String> records =
                    kafkaConsumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0


            logger.info("Received " + records.count() + " counts.");
            for (ConsumerRecord<String, String> record : records) {

                String json = record.value();

                //Two strategies to make consumer idempotent
                //Kafka generic id
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                //Twitter feed specific id
                String id = extractIdFromTweet(json);

                try {
                    IndexRequest indexRequest = new IndexRequest("twitter",
                            "tweets",
                            id)//This is to make our consumer idempotent
                            .source(json, XContentType.JSON);
                    IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                    id = indexResponse.getId();
                    logger.info("Id for data indexed in elastic index {} is {}", "twitter", id);
                    Thread.sleep(10);//produces some delay while consuming the messages from kafka topic to elastic search cluster
                } catch (ResponseException | ElasticsearchStatusException ex) {
                    logger.error("Failed to index the json value from twitter feed with string id {}", id);
                } catch (InterruptedException ex) {
                    logger.error("Waiting Sleep thread got interrupted!!", ex);
                }

            }

            logger.info("Committing the offset..");
            kafkaConsumer.commitSync();
            logger.info("Offsets have been committed");

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        //close the client gracefully
        //client.close();

    }

    public RestHighLevelClient createElasticClient() {
        logger.info("Creating elastic search client for REST!!");
        //////////////////////////
        /////////// IF YOU USE LOCAL ELASTICSEARCH
        //////////////////////////

        //  String hostname = "localhost";
        //  RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));


        //////////////////////////
        /////////// IF YOU USE BONSAI / HOSTED ELASTICSEARCH
        //Full Access:: https://g00zvd2s35:hz15u3cb9i@kafka-cluster-ajay-39942515.ap-southeast-2.bonsaisearch.net
        //////////////////////////

        // replace with your own credentials
        String hostname = "kafka-cluster-ajay-39942515.ap-southeast-2.bonsaisearch.net"; // localhost or bonsai url
        String username = "g00zvd2s35"; // needed
        String password = "hz15u3cb9i"; // needed  only for bonsaionly for bonsai

        // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public KafkaConsumer<String, String> createKafkaConsumer(String topic) {

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");//earliest
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    private static String extractIdFromTweet(String tweetJson) {
        final JsonParser jsonParser = new JsonParser();
        return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }
}
