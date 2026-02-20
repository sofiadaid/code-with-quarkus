package org.acme;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;

@QuarkusTest
public class TableRowsTest {

    @Test
    public void testInsertAndGetRows() {
        String table = "users_" + System.nanoTime();

        // créer table
        given()
                .contentType(ContentType.JSON)
                .body("""
                {
                  "name": "%s",
                  "columns": [
                    {"name":"id","type":"INT"},
                    {"name":"name","type":"STRING"},
                    {"name":"age","type":"INT"}
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
                  [1,"Sofia",21],
                  [2,"Ali",30],
                  [3,"Emma",25]
                ]
            """)
                .when()
                .post("/api/tables/%s/rows".formatted(table))
                .then()
                .statusCode(201)
                .body("inserted", equalTo(3));

        // 3️⃣ récupérer les lignes
        given()
                .when()
                .get("/api/tables/%s/rows".formatted(table))
                .then()
                .statusCode(200)
                .body("size()", equalTo(3))
                .body("[0][1]", equalTo("Sofia"))
                .body("[1][2]", equalTo(30));
    }
}
