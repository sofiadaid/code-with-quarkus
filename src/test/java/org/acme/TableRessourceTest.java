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

        // créer table
        given()
                .contentType(ContentType.JSON)
                .body("""
                        {
                          "name": "users",
                          "columns": [
                            {"name": "id", "type": "INT"},
                            {"name": "name", "type": "STRING"},
                            {"name": "age", "type": "INT"}
                          ]
                        }
                        """)
                .when()
                .post("/api/tables")
                .then()
                .statusCode(201);

        // insérer des lignes
        given()
                .contentType(ContentType.JSON)
                .body("""
                        [
                          [1,"Sofia",21],
                          [2,"Ania",30]
                        ]
                        """)
                .when()
                .post("/api/tables/users/rows")
                .then()
                .statusCode(201)
                .body("inserted", equalTo(2));
    }
}

