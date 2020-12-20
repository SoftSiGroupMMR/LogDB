package dk.si.logdb;


import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQConsumer {

    @Autowired
    private static Environment env;

    public static void main(String[] args) throws IOException, TimeoutException {

        rabbitConsumer();

    }

    public static void rabbitConsumer() throws IOException, TimeoutException {
        MongoClientURI uri = new MongoClientURI(env.getProperty("spring.data.mongodb.uri"));

        MongoClient mongoClient = new MongoClient(uri);
        MongoDatabase database = mongoClient.getDatabase("LogsSI");
        MongoCollection collection = database.getCollection("Logs");

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
            mongoDBProducer(message, collection);
        };
        channel.basicConsume("q_logstash", true, deliverCallback, consumerTag -> {
        });

    }

    private static void mongoDBProducer(String log, MongoCollection collection) {
        try {
            log.trim();
            Log logger = new Gson().fromJson(log, Log.class);
            Document document = new Document();
            document.append("klass", logger.getKlass());
            document.append("level", logger.getLevel());
            document.append("message", logger.getMessage());
            document.append("thread", logger.getThread());
            document.append("time", logger.getTime());
            collection.insertOne(document);
        } catch (Exception e) {
            System.out.println("Ilegal mapping, the message contains ilegal charset");
        }
    }
}
