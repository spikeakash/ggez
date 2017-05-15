package com.spnotes.kafka.simple;

import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
import com.sun.media.jfxmedia.events.PlayerStateEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;
import org.json.*;

/**
 * Created by sunilpatil on 12/28/15.
 */
public class Consumer {
    private static Scanner in;

    public static void main(String[] argv)throws Exception{
        if (argv.length != 2) {
            System.err.printf("Usage: %s <topicName> <groupId>\n",
                    Consumer.class.getSimpleName());
            System.exit(-1);
        }
        in = new Scanner(System.in);
        String topicName = argv[0];
        String groupId = argv[1];

        ConsumerThread consumerRunnable = new ConsumerThread(topicName,groupId);
        consumerRunnable.start();
        String line = "";
        while (!line.equals("exit")) {
            line = in.next();
        }
        consumerRunnable.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerRunnable.join();
    }

    private static class ConsumerThread extends Thread{
        private String topicName;
        private String groupId;
        private KafkaConsumer<String,String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId){
            this.topicName = topicName;
            this.groupId = groupId;
        }
        public void run() {
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");

            //Figure out where to start processing messages from
            kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));
            //Start processing messages
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                   // if(records.isEmpty()) System.out.println("trollolol");
                    //System.out.println(records.count());
                    for (ConsumerRecord<String, String> record : records) {
                       // System.out.println(record.value());
                        JSONParser parser = new JSONParser();
                        String str = record.value();
                        JSONObject jObj = null;
                        try {
                            jObj = (JSONObject) parser.parse(str);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        System.out.println("hi");

                        MongoClient mongoClient = new MongoClient();
                        DB db = mongoClient.getDB("test");
                        DBCollection persons = db.getCollection("Person");
                        String category = (String) jObj.get("category");
                        String name = (String) jObj.get("name");
                        long age = (Long) jObj.get("age");

                        if(category.equals("Professor")){
                            Final_Professor prof = new Final_Professor();
                            JSONArray arr = (JSONArray) jObj.get("subs");
                            prof.name = name;
                            prof.age = (int) age;
                            for(int i = 0; i < 4; i++) {
                                prof.subjectsTaught[i] = Integer.parseInt(String.valueOf(arr.get(i)));
                            }
                            prof.implementAllMethods();
                            System.out.println(prof);
                            // System.out.println("Total hours taught by " + prof.name + " is " + prof.sumOfHoursTaught);
                            Gson gson = new Gson();
                            BasicDBObject dbObj = (BasicDBObject) JSON.parse(gson.toJson(prof));
                            persons.insert(dbObj);
                        }else{
                            Final_Student stu = new Final_Student();
                            JSONArray arr = (JSONArray) jObj.get("subs");
                            stu.name = name;
                            stu.age = (int) age;
                            for(int i = 0; i < 4; i++){
                                stu.subjectsTaken[i] = Integer.parseInt((String.valueOf(arr.get(i))));
                            }
                            stu.implementAllMethods();
                            // System.out.println("Total marks of " + stu.name + " is " + stu.sumOfMarks);
                            Gson gson = new Gson();
                            BasicDBObject dbObj = (BasicDBObject) JSON.parse(gson.toJson(stu));
                            persons.insert(dbObj);
                        }
                        System.out.println("Done!!");


                        //System.out.println(record.value());
                    }
                }
            }catch(WakeupException ex){
                System.out.println("Exception caught " + ex.getMessage());
            }finally{
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }
        public KafkaConsumer<String,String> getKafkaConsumer(){
           return this.kafkaConsumer;
        }
    }
}

