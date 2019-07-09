package com.example.crudtwitter;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;


// Sem utilizar o String Deserializer tweet, usando o de String

public class ConsumerLifecycleManager implements LifecycleManager {

    private static Logger logger = LoggerFactory.getLogger(ConsumerLifecycleManager.class.getName());
    static KafkaConsumer<String,String> consumer = null;
    Tweet tweet = new Tweet();

    public Tweet converStringToTweet (String message) {
        String[] parts = message.split("-------");

        if (parts.length >= 9) {

            String createdAt = parts[0];
            String Id = parts[1];
            String Text = parts[2];
            String Source = parts[3];
            String IsTruncated = parts[4];
            String Latitude = parts[5];
            String Longitude = parts[6];
            String IsFavorited = parts[7];
            String User = parts[8];
            String Language = parts[9];

            System.out.println("Read tweet from topic:");
            System.out.println(createdAt);
            System.out.println(Id);
            System.out.println(Text);
            System.out.println(Source);
            System.out.println(IsTruncated);
            System.out.println(Latitude);
            System.out.println(Longitude);
            System.out.println(IsFavorited);
            System.out.println(User);
            System.out.println(Language);
            System.out.println("\n");

            tweet = new Tweet(createdAt,
                    (long) Long.valueOf(Id).longValue(),
                    Text,
                    Source,
                    IsTruncated,
                    Latitude,
                    Longitude,
                    IsFavorited,
                    User,
                    Language
            );
            return tweet;
        }
        return null;
    }


    public static KafkaConsumer<String,String> getConsumerInstance(){
        // Criar as propriedades do consumidor
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "tweet-application");


        //TODO DESERIALIZER
//        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TweetDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer .class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Criar o consumidor
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        return consumer;
    }

    public void start(){
        if (consumer == null) {
            consumer = getConsumerInstance();
            // Subscrever o consumidor para o nosso(s) tópico(s)
//            consumer.subscribe(Collections.singleton("kafka_consumer_topic"));
            consumer.subscribe(Collections.singleton("tweets_input"));
        }
        int i = 0;
        List<Tweet> tweetBuffer = new ArrayList<Tweet>();
//        while (consumer != null) {  // Apenas como demo, usaremos um loop infinito
        while (i < 5) {  // Apenas como demo, usaremos um loop infinito
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord record : poll) {
                Tweet temp = converStringToTweet(record.value().toString());
                if (temp != null) {
                    tweetBuffer.add(temp);
                    i++;
                    System.out.println("Consumer got tweet!");
                }
            }

        }


        Cluster cluster = null;

        try {
            cluster = Cluster.builder().addContactPoint("localhost").build();
            Session session = cluster.connect();

            ResultSet rs = session.execute("Select release_version from system.local");
            Row row = rs.one();
            System.out.println(row.getString("release_version"));
            KeyspaceRepository sr = new KeyspaceRepository(session);

            //Cria keyspace
            sr.createKeyspace("library", "SimpleStrategy", 1);
            System.out.println("Create repository");

            sr.useKeyspace("library");
            System.out.println("Using repository library");

            // Cria tabela tweets
            TweetRepository tweets = new TweetRepository(session);
            tweets.createTable();
            System.out.println("Create table tweets");

            // Cria tabela tweetsByLang
            tweets.createTableTweetsByLang();
            System.out.println("Create table tweetsByLang - step 2");

            for (int j = 0; j < 5; j++) {
                Tweet tw = tweetBuffer.get(j);
                tweets.insertTweet(tw);
                tweets.insertTweetsByLang(tw);
                System.out.println("Inserted tweet in tables");
            }

            tweets.selectAll(); // Apresenta todas as tuplas
            tweets.selectAllTweetsByLang(); // Apresenta todas as tuplas
            tweets.selectAllTweetsByLang(); // Apresenta todas as tuplas

            tweets.deleteTable("tweets");
            System.out.println("Deleted tweets");

            sr.deleteKeyspace("library");
            System.out.println("Delete keyspace library");

        } finally {
            if (cluster != null) cluster.close();
        }


        System.exit(0);
    }

    public void stop(){
        // Close Producer
        consumer.close();
        consumer = null;
    }

}




// Utilizando o String Deserializer com Tweet
/*

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.serialization.StringDeserializer;


public class ConsumerLifecycleManager implements LifecycleManager, Serializable {
    private static final String KAFKA_CLUSTER = System.getenv().getOrDefault("KAFKA_CLUSTER", "localhost:9092");
    private static final String CONSUMER_GROUP = "tweet-application";
    private static final String TOPIC_NAME = "tweets_input";
    private static final Logger logger = LoggerFactory.getLogger(ConsumerLifecycleManager.class.getName());
    private final AtomicBoolean running = new AtomicBoolean(false);
    private KafkaConsumer<String, Tweet> consumer;
    private Future<?> future;

    public ConsumerLifecycleManager() {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER);
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TweetDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(kafkaProps);
    }

    public void start()  {
        if (running.compareAndSet(false, true)) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            future = executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        consumer.subscribe(Arrays.asList(TOPIC_NAME));
                        logger.info("Consumidor subscrito no tópico: ", TOPIC_NAME);
                        while (true) {
                            ConsumerRecords<String, Tweet> records = consumer.poll(Duration.ofMillis(1000));
                            for (ConsumerRecord<String, Tweet> record : records) {
                                Tweet tweet = (Tweet) record.value();
                                logger.info("Consumindo do Kafka o Tweet: " + tweet);
                            }
                        }
                    } catch (WakeupException e) {
                        // ignora
                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.error ("Erro no consumo dos tweets do Kafka", e);
                    } finally {
                        consumer.close();
                    }
                }
            });
            logger.info("Serviço iniciado");
        } else {
            logger.warn("O serviço já está executando.");
        }
    }

    public void stop()  {
        if (running.compareAndSet(true, false)) {
            if (future.cancel(true)) {
                consumer.wakeup();
            }
            logger.info("Serviço finalizado");
        } else {
            logger.warn("O serviço não está executando. Não pode ser parado.");
        }
    }

}

*/