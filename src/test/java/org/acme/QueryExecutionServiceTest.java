package org.acme;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.*;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;

@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class QueryExecutionServiceTest {

    // Nom unique par JVM — évite les conflits entre classes de test
    static final String TABLE = "test_query";

    @Test
    @Order(1)
    void test_01_create_and_insert() {
        // Créer la table
        given()
                .contentType(ContentType.JSON)
                .body("""
                        {
                          "name": "%s",
                          "columns": [
                            {"name": "name", "type": "STRING"},
                            {"name": "age",  "type": "INT"},
                            {"name": "city", "type": "STRING"}
                          ]
                        }
                        """.formatted(TABLE))
                .when().post("/api/tables")
                .then().statusCode(201);

        // Insérer les données
        given()
                .contentType(ContentType.JSON)
                .body("""
                        [
                          ["Alice",   22, "Paris"],
                          ["Bob",     19, "Lyon"],
                          ["Charlie", 25, "Paris"]
                        ]
                        """)
                .when().post("/api/tables/" + TABLE + "/rows")
                .then().statusCode(201).body("inserted", equalTo(3));
    }

    @Test
    @Order(2)
    void test_02_where_equal() {
        given()
                .contentType(ContentType.JSON)
                .body("""
                        {
                          "columns": ["name", "city"],
                          "filter": {
                            "column": "city",
                            "operator": "=",
                            "value": "Paris"
                          }
                        }
                        """)
                .when().post("/api/tables/" + TABLE + "/select-where")
                .then()
                .statusCode(200)
                .body("size()", is(2))
                .body("[0][0]", equalTo("Alice"))
                .body("[1][0]", equalTo("Charlie"));
    }

    @Test
    @Order(3)
    void test_03_where_greater_than() {
        given()
                .contentType(ContentType.JSON)
                .body("""
                        {
                          "columns": ["name", "age"],
                          "filter": {
                            "column": "age",
                            "operator": ">",
                            "value": 20
                          }
                        }
                        """)
                .when().post("/api/tables/" + TABLE + "/select-where")
                .then()
                .statusCode(200)
                .body("size()", is(2));
    }

    @Test
    @Order(4)
    void test_04_where_invalid_column() {
        given()
                .contentType(ContentType.JSON)
                .body("""
                        {
                          "columns": ["name"],
                          "filter": {
                            "column": "unknown",
                            "operator": "=",
                            "value": "Paris"
                          }
                        }
                        """)
                .when().post("/api/tables/" + TABLE + "/select-where")
                .then()
                .statusCode(400);
    }

    @Test
    @Order(5)
    void test_05_groupby_count() {
        given()
                .when().get("/api/tables/" + TABLE + "/group-by?column=city")
                .then()
                .statusCode(200)
                .body("size()", is(2));  // Paris (2) et Lyon (1)
    }

    @Test
    @Order(6)
    void test_06_sql_where_order_limit() {
        given()
                .when().get("/api/tables/" + TABLE + "/query?q=SELECT * WHERE age > 18 ORDER BY age LIMIT 2")
                .then()
                .statusCode(200)
                .body("size()", is(2));
    }
}
