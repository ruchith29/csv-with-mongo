package com.nextrow.csvtomongodb.controller;

import com.mongodb.client.*;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.bson.Document;

import org.springframework.web.bind.annotation.*;

import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@RestController
public class Controller {

    @GetMapping("/storeData")
    public void storeData() throws IOException, CsvValidationException {
        // to count the number of tables created
        int count = 0;

        try
        {
            // reading the csv file
            CSVReader csvReader = new CSVReader(new FileReader("movies.csv"));

            // storing the first line which consists of headers
            String headers[];
            headers = csvReader.readNext();

            // mongodb connection
            MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
            MongoDatabase mongoDatabase = mongoClient.getDatabase("movies");

            // array to store the values for each row
            String tableData[];

            // iterating the file
            while ((tableData = csvReader.readNext())!= null) {
                // variable for header array to assign values
                int i=0;

                // storing the title of movie without any special chars
                String title="_"+tableData[11].replace(".", "_").replace("$", "_");

                // map to store the headers as keys and its values
                HashMap<String, String> tableValues = new HashMap<>();

                // iterating over each header for every row to store each row as a unique table
                for (String header : headers) {

                    // if header is named as title then will create the table else will just add these values to map
                    if (header.equals("title")) {
                        mongoDatabase.createCollection(title);
                        System.out.print("Table: " + title + " is created");

                    } else {
                        tableValues.put(headers[i], tableData[i]);
                    }
                    i++;// inc for each column and again becomes 0 for each iteration
                }
                count++;// inc for table count

                // to store the values of table into db
                MongoCollection<Document>  mongoCollection=mongoDatabase.getCollection(title);
                Document document=new Document(tableValues);
                mongoCollection.insertOne(document);

                System.out.print(" and Values added\n");
            }
        }

        catch (Exception e)
        {
            System.out.println("Exception: "+e);
        }
        System.out.println("Table count: "+count);
    }


    @GetMapping("/api/v1/movie")
    public List<Document> getData(@RequestParam(value = "name") String title){

        // mongodb connection
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase mongoDatabase = mongoClient.getDatabase("movies");

        // storing the title of movie without any special chars as stored in db
        String name="_"+title.replace(".", "_").replace("$", "_");

        // fetching all the data to collection with the user specified movie name
        MongoCollection<Document> mongoCollection=mongoDatabase.getCollection(name);

        // to store the data and return to user
        List<Document> documents = new ArrayList<>();

        // iterating over doc to add data to list
        for (Document document : mongoCollection.find()) {
            document.remove("_id");
            documents.add(document);
        }

        // returning list with all the info of user specified movie name
        return documents;
    }
}
