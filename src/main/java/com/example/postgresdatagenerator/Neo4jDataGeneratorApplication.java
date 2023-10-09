package com.example.postgresdatagenerator;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;

@SpringBootApplication
@Slf4j
public class Neo4jDataGeneratorApplication implements CommandLineRunner {

    private final Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "12345678"));

    private static final int USERS_AMOUNT = 1000;
    private static final int FRIENDS_LIST_SIZE = 100;

    List<String> names = List.of(
            "Mike",
            "Peter",
            "Ivan",
            "John",
            "Sergey",
            "Pavel",
            "Jacob"
    );

    public static void main(String[] args) {
        SpringApplication.run(Neo4jDataGeneratorApplication.class, args);
    }

    @Override
    public void run(String... args) {
        try (Session session = driver.session(SessionConfig.forDatabase("testdata"))) {
            prepareDatabase(session);
            insertUsers(session);
            insertFollowers(session);
            for (int i = 1; i < 4; i++) {
                testJoinLevel(session, i);
            }
        }
    }

    @SneakyThrows
    private void prepareDatabase(Session session) {
        session.run("MATCH(n) DETACH DELETE n;");
        session.run("DROP CONSTRAINT uniqueRelationship IF EXISTS");
        session.run("CREATE CONSTRAINT uniqueRelationship FOR ()-[r:FOLLOWS]->() REQUIRE (r.property) IS UNIQUE;");
        log.info("Database is loaded.");
    }

    @SneakyThrows
    private void insertUsers(Session session) {
        System.out.print("Insertion started");
        long start = System.currentTimeMillis();
        String query = "CREATE (:User {id: $id, name: $name});";
        for (int i = 0; i < USERS_AMOUNT; i++) {
            int index = (int) (Math.random() * (names.size() - 1));
            Value values = Values.parameters("id", i + 1, "name", names.get(index));
            session.run(query, values);
            if (i % (USERS_AMOUNT / 100) == 0) {
                System.out.print(".");
            }
        }
        long end = System.currentTimeMillis();
        System.out.println();
        log.info("{} users were inserted in {} s.", USERS_AMOUNT, (end - start) / 1000.0);
    }

    @SneakyThrows
    private void insertFollowers(Session session) {
        System.out.print("Follower insertion started");
        long start = System.currentTimeMillis();
        String query = "MATCH (u1:User), (u2:User) WHERE u1.id = $firstId AND u2.id = $secondId CREATE (u1)-[:FOLLOWS]->(u2);";
        for (int i = 0; i < USERS_AMOUNT; i++) {
            for (int j = 0; j < FRIENDS_LIST_SIZE; j++) {
                int index = (int) (Math.random() * (USERS_AMOUNT - 1));
                if (index == i) {
                    j--;
                } else {
                    Value values = Values.parameters("firstId", i + 1, "secondId", index + 1);
                    session.run(query, values);
                }
            }
            if (i % (USERS_AMOUNT / 100) == 0) {
                System.out.print(".");
            }
        }
        long end = System.currentTimeMillis();
        System.out.println();
        log.info("{} friends were inserted in {} s.", FRIENDS_LIST_SIZE * USERS_AMOUNT, (end - start) / 1000.0);
    }

    @SneakyThrows
    private void testJoinLevel(Session session, int level) {
        StringBuilder query = new StringBuilder("MATCH (n1)-[:FOLLOWS]->(n2)");
        for (int i = 2; i <= level; i++) {
            query.append("-[:FOLLOWS]->(n").append(i + 1).append(")");
        }
        query.append("\nWHERE n1.id = $id\n");
        query.append("RETURN n").append(level + 1).append(".id as id, n").append(level + 1).append(".name as name;");
        long start = System.currentTimeMillis();
        int index = (int) (Math.random() * (USERS_AMOUNT - 1));
        Value values = Values.parameters("id", index + 1);
        Result result = session.run(query.toString(), values);
        while (result.hasNext()) {
            Record record = result.next();
            User user = new User(record.get("id").asLong(), record.get("name").asString());
        }
        long end = System.currentTimeMillis();
        log.info("Retrieve {} {} level join objects in {} s", (int) Math.pow(FRIENDS_LIST_SIZE, level), level, (end - start) / 1000.0);
    }

}
