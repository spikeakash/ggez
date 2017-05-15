package com.spnotes.kafka.simple;

import com.google.gson.Gson;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;

import com.mongodb.ServerAddress;

/**
 * Created by akash.ka on 5/11/17.
 */
public class testerForJSON {
    public static void main(String[] args) throws FileNotFoundException {

        JSONParser parser = new JSONParser();
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("test");
        DBCollection persons = db.getCollection("Person");
        persons.drop();
        persons = db.getCollection("Person");
       // DBCollection studentColl = db.getCollection("Student");
       // DBCollection profColl = db.getCollection("Professor");
/*
        try {
            JSONArray mainArr = (JSONArray) parser.parse(new FileReader("/Users/akash.ka/Desktop/jsonFile/records.json"));
            for(Object obj : mainArr){

                JSONObject jObj = (JSONObject) obj;
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
            }
        } catch (IOException e) {
            System.out.println("catch 1");
            e.printStackTrace();
        } catch (ParseException e) {
            System.out.println("catch 2");
            e.printStackTrace();
        }
        */

    }

}
