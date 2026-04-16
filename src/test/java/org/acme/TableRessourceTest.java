package org.acme;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;

@QuarkusTest
public class TableRessourceTest {

    @Test
    public void testCreateTableAndInsertRows() {
        String table = "users_resource_" + System.nanoTime();

        // créer table
        given()
                .contentType(ContentType.JSON)
                .body("""
                        {
                          "name": "%s",
                          "columns": [
                            {"name": "id",   "type": "INT"},
                            {"name": "name", "type": "STRING"},
                            {"name": "age",  "type": "INT"}
                          ]
                        }
                        """.formatted(table))
                .when()
                .post("/api/tables")
                .then()
                .statusCode(201);

        // insérer des lignes
        given()
                .contentType(ContentType.JSON)
                .body("""
                        [
                          [1, "Sofia", 21],
                          [2, "Ania",  30]
                        ]
                        """)
                .when()
                .post("/api/tables/" + table + "/rows")
                .then()
                .statusCode(201)
                .body("inserted", equalTo(2));
    }

    @Test
    public void testCreateTableAlreadyExists() {
        String table = "duplicate_" + System.nanoTime();

        // première création → 201
        given()
                .contentType(ContentType.JSON)
                .body("""
                        {
                          "name": "%s",
                          "columns": [{"name": "id", "type": "INT"}]
                        }
                        """.formatted(table))
                .when().post("/api/tables")
                .then().statusCode(201);

        // deuxième création → 400
        given()
                .contentType(ContentType.JSON)
                .body("""
                        {
                          "name": "%s",
                          "columns": [{"name": "id", "type": "INT"}]
                        }
                        """.formatted(table))
                .when().post("/api/tables")
                .then().statusCode(400);
    }

    @Test
    public void testGetTableNotFound() {
        given()
                .when().get("/api/tables/table_inexistante_xyz")
                .then().statusCode(404);
    }
}
