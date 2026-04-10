package org.acme;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;

@QuarkusTest
public class QueryExecutionServiceTest {

    @Test
    void test_where_equal() {

        // 1. Créer table
        given()
                .contentType(ContentType.JSON)
                .body("""
                        {
                          "name": "users",
                          "columns": [
                            {"name": "name", "type": "STRING"},
                            {"name": "age", "type": "INT"},
                            {"name": "city", "type": "STRING"}
                          ]
                        }
                        """)
                .when().post("/api/tables")
                .then().statusCode(201);

        // 2. Insérer des données
        given()
                .contentType(ContentType.JSON)
                .body("""
                        [
                          ["Alice", 22, "Paris"],
                          ["Bob", 19, "Lyon"],
                          ["Charlie", 25, "Paris"]
                        ]
                        """)
                .when().post("/api/tables/users/rows")
                .then().statusCode(201);

        // 3. Tester WHERE city = Paris
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
                .when().post("/api/tables/users/select-where")
                .then()
                .statusCode(200)
                .body("$.size()", is(2))
                .body("[0][0]", equalTo("Alice"))
                .body("[1][0]", equalTo("Charlie"));
    }

    @Test
    void test_where_greater_than() {

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
                .when().post("/api/tables/users/select-where")
                .then()
                .statusCode(200)
                .body("$.size()", is(2));
    }

    @Test
    void test_where_invalid_column() {

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
                .when().post("/api/tables/users/select-where")
                .then()
                .statusCode(400);
    }
}