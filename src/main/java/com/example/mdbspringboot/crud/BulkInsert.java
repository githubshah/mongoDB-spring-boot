package com.example.mdbspringboot.crud;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import org.bson.Document;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

@Component
public class BulkInsert {

    @PostConstruct
    public static void bulkInsert() {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false"))
                .build();

        MongoClient mongoClient = MongoClients.create(settings);

        WriteConcern wc = new WriteConcern(0).withJournal(false);

        String databaseName = "test";
        String collectionName = "testCollection";

        System.out.println("Database: " + databaseName);
        System.out.println("Collection: " + collectionName);
        System.out.println("Write concern: " + wc);

        MongoDatabase database = mongoClient.getDatabase(databaseName);

        MongoCollection<Document> collection = database.getCollection(collectionName).withWriteConcern(wc);

        collection.deleteMany(new Document());

        int rows = 1000000;
        int iterations = 5;
        int batchSize = 1000;

        double accTime = 0;

        database.drop();

        for (int it = 0; it < iterations; it++) {

            List<InsertOneModel<Document>> docs = new ArrayList<>();

            int batch = 0;
            long totalTime = 0;

            for (int i = 0; i < rows; ++i) {

                docs.add(new InsertOneModel<>(populateDoc()));
                batch++;

                if (batch >= batchSize) {
                    long start = System.currentTimeMillis();
                    collection.bulkWrite(docs);
                    totalTime += System.currentTimeMillis() - start;

                    docs.clear();
                    batch = 0;
                }
            }

            if (batch > 0) {
                long start = System.currentTimeMillis();

                collection.bulkWrite(docs);

                totalTime += System.currentTimeMillis() - start;

                docs.clear();
            }

            accTime += totalTime;

            System.out.println(">> Iteration " + it + " - Elapsed: " + (totalTime / 1000.0) + " seconds.");
        }

        System.out.println("Avg: " + ((accTime / 1000.0) / iterations) + " seconds.");

        mongoClient.close();
    }

    public static Document populateDoc() {
        String key1 = "7";
        String key2 = "8395829";
        String key3 = "928749";
        String key4 = "9";
        String key5 = "28";
        String key6 = "44923.59";
        String key7 = "0.094";
        String key8 = "0.29";
        String key9 = "e";
        String key10 = "r";
        String key11 = "2020-03-16";
        String key12 = "2020-03-16";
        String key13 = "2020-03-16";
        String key14 = "klajdlfaijdliffna";
        String key15 = "933490";
        String key17 = "paorgpaomrgpoapmgmmpagm";

        return new Document("key17", key17).append("key12", key12).append("key7", key7)
                .append("key6", key6).append("key4", key4).append("key10", key10).append("key1", key1)
                .append("key2", key2).append("key5", key5).append("key13", key13).append("key9", key9)
                .append("key11", key11).append("key14", key14).append("key15", key15).append("key3", key3)
                .append("key8", key8);
    }
}
