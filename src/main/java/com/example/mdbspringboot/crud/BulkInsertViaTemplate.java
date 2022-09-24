package com.example.mdbspringboot.crud;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

@Component
public class BulkInsertViaTemplate {

    @Autowired
    MongoTemplate mongoTemplate;

    @PostConstruct
    public void setUp() {

        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, "test1");

        List<WriteModel<Document>> list = new ArrayList<>();
        for (int i = 1; i < 10; i++) {
            Document document = new Document("postId", i)
                    .append("name", "id labore ex et quam laborum")
                    .append("email", "Eliseo@gardner.biz")
                    .append("secondary-email", "Eliseo@gardner.biz")
                    .append("body", "laudantium enim quasi est");
            list.add(new InsertOneModel<>(document));
        }

        bulkOperations.insert(list);
        bulkOperations.execute();
    }
}
