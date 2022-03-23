package com.sailthru.docdb;

import com.mongodb.client.*;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.bson.conversions.Bson;

import static java.lang.System.exit;

public class SetOnInsertDocdb {

    private static final Bson FILTER_QUERY_1 = new Document("_id", "my_id_1");


    public static void main(String[] args) {

        checkArgsAndPrintUsage(args);

        try(MongoClient client = MongoClients.create(args[0])) {

            MongoCollection<Document> docs = getDocumentMongoCollection(client);
            docs.findOneAndDelete(FILTER_QUERY_1);

            System.out.println("\nCreate document with setOnInsert on ID");
            Bson updateQuery = new Document("$setOnInsert", new Document("_id", "my_id_1"));
            docs.updateOne(FILTER_QUERY_1, updateQuery, (new UpdateOptions()).upsert(true));
            printTrackedEntry(docs);

            System.out.println("Upsert existing document with setOnInsert on ID");
            docs.updateOne(FILTER_QUERY_1, updateQuery, (new UpdateOptions()).upsert(true));
            printTrackedEntry(docs);
        }
    }

    private static MongoCollection<Document> getDocumentMongoCollection(MongoClient client) {
        MongoDatabase db = client.getDatabase("sailthru");
        return db.getCollection("my_collection");
    }

    private static void printTrackedEntry(MongoCollection<Document> docs) {
        FindIterable<Document> documents = docs.find(FILTER_QUERY_1);

        if (documents.first() != null) {
            for (Document document : documents) {
                System.out.printf("DOCUMENT: %s%n", document.toJson());
            }
        } else {
            System.out.printf("DOCUMENT: %s%n", "NOT FOUND");
        }
        System.out.println();
    }

    private static void checkArgsAndPrintUsage(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage  : ./gradlew run --args mongodb://[username:password@]host1[:port1]/[database][?options]");
            System.out.println("Example: ./gradlew run --args mongodb://localhost");
            exit(0);
        }
    }
}
