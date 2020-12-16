package dk.si.logdb;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.bson.Document;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RabbitMQConsumer {
    private ObjectMapper objectMapper = new ObjectMapper();
    private static int count = 0;
    public static void main(String[] args) throws IOException, TimeoutException {

        rabbitConsumer();

    }

    public static void rabbitConsumer() throws IOException, TimeoutException {
        MongoClientURI uri = new MongoClientURI(
                "mongodb+srv://mmmrj1:mmmrj1@cluster0.anvpi.mongodb.net/LogsSI?retryWrites=true&w=majority");

        MongoClient mongoClient = new MongoClient(uri);
        MongoDatabase database = mongoClient.getDatabase("LogsSI");


        ConnectionFactory factory = new ConnectionFactory();

        factory.setHost("188.166.16.16");
        factory.setUsername("mmmrj1");
        factory.setPassword("mmmrj1");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare("q_logstash", true, false, false, null);


        DeliverCallback deliverCallback = (consumerTag, delivery) ->
        {
            String message = new String(delivery.getBody(), "UTF-8");
            mongoDBProducer(message, database);
        };
        channel.basicConsume("q_logstash", true, deliverCallback, consumerTag -> {
        });

    }

    private static void mongoDBProducer(String log, MongoDatabase database) throws JsonProcessingException {
        MongoCollection collection = database.getCollection("Logs");
        Map<String, Object> map = new ObjectMapper().readValue(log.replaceAll("\"",""), new TypeReference<Map<String, Object>>() {
        });
        System.out.println("IN"+count);
        Document document = new Document();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            document.append(entry.getKey(), entry.getValue());
        }
        System.out.println("OUT"+count);
        collection.insertOne(document);
        System.out.println("SEND"+count);
        count++;
    }
}
