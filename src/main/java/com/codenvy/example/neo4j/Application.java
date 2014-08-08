package com.codenvy.example.neo4j;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;

public class Application {
    private static final String DB_PATH = "target/neo4j-hello-db";

    public String greeting;

    GraphDatabaseService graphDb;
    Node                 firstNode;
    Node                 secondNode;
    Relationship         relationship;

    private static enum RelTypes implements RelationshipType {
        KNOWS
    }

    public static void main(final String[] args) {
        Application hello = new Application();
        hello.createDb();
        hello.removeData();
        hello.shutDown();
    }

    void createDb() {
        deleteFileOrDirectory(new File(DB_PATH));

        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
        registerShutdownHook(graphDb);

        try (Transaction tx = graphDb.beginTx()) {
            firstNode = graphDb.createNode();
            firstNode.setProperty("message", "Hello, ");
            secondNode = graphDb.createNode();
            secondNode.setProperty("message", "World!");

            relationship = firstNode.createRelationshipTo(secondNode, RelTypes.KNOWS);
            relationship.setProperty("message", "brave Neo4j ");

            System.out.print(firstNode.getProperty("message"));
            System.out.print(relationship.getProperty("message"));
            System.out.print(secondNode.getProperty("message"));

            greeting = ((String)firstNode.getProperty("message"))
                       + ((String)relationship.getProperty("message"))
                       + ((String)secondNode.getProperty("message"));

            tx.success();
        }
    }

    void removeData() {
        try (Transaction tx = graphDb.beginTx()) {
            firstNode.getSingleRelationship(RelTypes.KNOWS, Direction.OUTGOING).delete();
            firstNode.delete();
            secondNode.delete();
            tx.success();
        }
    }

    void shutDown() {
        System.out.println();
        System.out.println("Shutting down database ...");
        graphDb.shutdown();
    }

    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }

    private static void deleteFileOrDirectory(File file) {
        if (file.exists()) {
            if (file.isDirectory()) {
                for (File child : file.listFiles()) {
                    deleteFileOrDirectory(child);
                }
            }
            file.delete();
        }
    }
}
