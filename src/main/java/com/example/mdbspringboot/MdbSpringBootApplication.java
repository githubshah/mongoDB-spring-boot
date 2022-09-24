package com.example.mdbspringboot;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import com.example.mdbspringboot.model.GroceryItem;
import com.example.mdbspringboot.repository.CustomItemRepository;
import com.example.mdbspringboot.repository.ItemRepository;

@SpringBootApplication
@EnableMongoRepositories
public class MdbSpringBootApplication implements CommandLineRunner{
	
	@Autowired
	ItemRepository groceryItemRepo;
	
	@Autowired
	CustomItemRepository customRepo;
	
	List<GroceryItem> itemList = new ArrayList<GroceryItem>();

	public static void main(String[] args) {
		SpringApplication.run(MdbSpringBootApplication.class, args);
	}
	
	public void run(String... args) {

		bulkInsert2();

		if(true)return;
		
		// Clean up any previous data
		groceryItemRepo.deleteAll(); // Doesn't delete the collection
		
		System.out.println("-------------CREATE GROCERY ITEMS-------------------------------\n");
		
		createGroceryItems();
		
		System.out.println("\n----------------SHOW ALL GROCERY ITEMS---------------------------\n");
		
		showAllGroceryItems();
		
		System.out.println("\n--------------GET ITEM BY NAME-----------------------------------\n");
		
		getGroceryItemByName("Whole Wheat Biscuit");
		
		System.out.println("\n-----------GET ITEMS BY CATEGORY---------------------------------\n");
		
		getItemsByCategory("millets");
		
		System.out.println("\n-----------UPDATE CATEGORY NAME OF ALL GROCERY ITEMS----------------\n");
		
		updateCategoryName("snacks");
		
		System.out.println("\n-----------UPDATE QUANTITY OF A GROCERY ITEM------------------------\n");
		
		updateItemQuantity("Bonny Cheese Crackers Plain", 10);
		
		System.out.println("\n----------DELETE A GROCERY ITEM----------------------------------\n");
		
		deleteGroceryItem("Kodo Millet");
		
		System.out.println("\n------------FINAL COUNT OF GROCERY ITEMS-------------------------\n");
		
		findCountOfGroceryItems();
		
		System.out.println("\n-------------------THANK YOU---------------------------");
						
	}
	
	// CRUD operations

	//CREATE
	void createGroceryItems() {
		System.out.println("Data creation started...");

		groceryItemRepo.save(new GroceryItem("Whole Wheat Biscuit", "Whole Wheat Biscuit", 5, "snacks"));
		groceryItemRepo.save(new GroceryItem("Kodo Millet", "XYZ Kodo Millet healthy", 2, "millets"));
		groceryItemRepo.save(new GroceryItem("Dried Red Chilli", "Dried Whole Red Chilli", 2, "spices"));
		groceryItemRepo.save(new GroceryItem("Pearl Millet", "Healthy Pearl Millet", 1, "millets"));
		groceryItemRepo.save(new GroceryItem("Cheese Crackers", "Bonny Cheese Crackers Plain", 6, "snacks"));
		
		System.out.println("Data creation complete...");
	}
	
	// READ
	// 1. Show all the data
	 public void showAllGroceryItems() {
		 
		 itemList = groceryItemRepo.findAll();
		 
		 itemList.forEach(item -> System.out.println(getItemDetails(item)));
	 }
	 
	 // 2. Get item by name
	 public void getGroceryItemByName(String name) {
		 System.out.println("Getting item by name: " + name);
		 GroceryItem item = groceryItemRepo.findItemByName(name);
		 System.out.println(getItemDetails(item));
	 }
	 
	 // 3. Get name and items of a all items of a particular category
	 public void getItemsByCategory(String category) {
		 System.out.println("Getting items for the category " + category);
		 List<GroceryItem> list = groceryItemRepo.findAll(category);
		 
		 list.forEach(item -> System.out.println("Name: " + item.getName() + ", Quantity: " + item.getItemQuantity()));
	 }
	 
	 // 4. Get count of documents in the collection
	 public void findCountOfGroceryItems() {
		 long count = groceryItemRepo.count();
		 System.out.println("Number of documents in the collection = " + count);
	 }
	 
	 // UPDATE APPROACH 1: Using MongoRepository
	 public void updateCategoryName(String category) {
		 
		 // Change to this new value
		 String newCategory = "munchies";
		 
		 // Find all the items with the category 
		 List<GroceryItem> list = groceryItemRepo.findAll(category);
		 
		 list.forEach(item -> {
			 // Update the category in each document
			 item.setCategory(newCategory);
		 });
		 
		 // Save all the items in database
		 List<GroceryItem> itemsUpdated = groceryItemRepo.saveAll(list);
		 
		 if(itemsUpdated != null)
			 System.out.println("Successfully updated " + itemsUpdated.size() + " items.");		 
	 }
	 
	 
	 // UPDATE APPROACH 2: Using MongoTemplate
	 public void updateItemQuantity(String name, float newQuantity) {
		 System.out.println("Updating quantity for " + name);
		 customRepo.updateItemQuantity(name, newQuantity);
	 }
	 
	 // DELETE
	 public void deleteGroceryItem(String id) {
		 groceryItemRepo.deleteById(id);
		 System.out.println("Item with id " + id + " deleted...");
	 }
	 // Print details in readable form
	 
	 public String getItemDetails(GroceryItem item) {

		 System.out.println(
		 "Item Name: " + item.getName() + 
		 ", \nItem Quantity: " + item.getItemQuantity() + 
		 ", \nItem Category: " + item.getCategory()
		 );
		 
		 return "";
	 }


	public static void bulkInsert2() {
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


	public static Document populateDoc(){
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

