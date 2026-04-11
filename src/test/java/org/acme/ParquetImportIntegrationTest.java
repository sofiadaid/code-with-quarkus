package org.acme;

import io.quarkus.test.junit.QuarkusTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.*;

import java.io.*;
import java.nio.file.Files;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;

/**
 * Test d'intégration end-to-end :
 *   génère un fichier Parquet → import via API → SELECT / WHERE / GROUP BY
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParquetImportIntegrationTest {

    static final String TABLE = "taxi_test";
    static File parquetFile;

    // OutputFile local sans dépendance Hadoop MapReduce
    static class LocalOutputFile implements OutputFile {
        private final File file;
        LocalOutputFile(File file) { this.file = file; }

        private PositionOutputStream openStream() throws IOException {
            FileOutputStream fos = new FileOutputStream(file);
            return new PositionOutputStream() {
                long pos = 0;
                @Override public long getPos() { return pos; }
                @Override public void write(int b) throws IOException { fos.write(b); pos++; }
                @Override public void write(byte[] b, int off, int len) throws IOException { fos.write(b, off, len); pos += len; }
                @Override public void flush() throws IOException { fos.flush(); }
                @Override public void close() throws IOException { fos.close(); }
            };
        }

        @Override public PositionOutputStream create(long blockSizeHint) throws IOException { return openStream(); }
        @Override public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException { return openStream(); }
        @Override public boolean supportsBlockSize() { return false; }
        @Override public long defaultBlockSize() { return 0; }
    }

    // Builder ParquetWriter sans MapReduce
    static class GroupWriterBuilder extends ParquetWriter.Builder<Group, GroupWriterBuilder> {
        GroupWriterBuilder(OutputFile file) { super(file); }
        @Override protected GroupWriterBuilder self() { return this; }
        @Override protected org.apache.parquet.hadoop.api.WriteSupport<Group> getWriteSupport(Configuration conf) {
            return new GroupWriteSupport();
        }
    }

    @BeforeAll
    static void generateParquetFile() throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType("""
                message taxi {
                  required int64  vendor_id;
                  required double trip_distance;
                  required double fare_amount;
                  required int64  passenger_count;
                  required binary payment_type (UTF8);
                }
                """);

        parquetFile = Files.createTempFile("taxi_test_", ".parquet").toFile();
        parquetFile.deleteOnExit();

        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        try (ParquetWriter<Group> writer = new GroupWriterBuilder(new LocalOutputFile(parquetFile))
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()) {

            writer.write(factory.newGroup()
                    .append("vendor_id", 1L).append("trip_distance", 2.5)
                    .append("fare_amount", 10.0).append("passenger_count", 2L)
                    .append("payment_type", "Credit"));
            writer.write(factory.newGroup()
                    .append("vendor_id", 2L).append("trip_distance", 5.0)
                    .append("fare_amount", 20.0).append("passenger_count", 1L)
                    .append("payment_type", "Cash"));
            writer.write(factory.newGroup()
                    .append("vendor_id", 1L).append("trip_distance", 1.2)
                    .append("fare_amount", 7.5).append("passenger_count", 3L)
                    .append("payment_type", "Credit"));
            writer.write(factory.newGroup()
                    .append("vendor_id", 2L).append("trip_distance", 8.0)
                    .append("fare_amount", 35.0).append("passenger_count", 1L)
                    .append("payment_type", "Credit"));
            writer.write(factory.newGroup()
                    .append("vendor_id", 1L).append("trip_distance", 0.8)
                    .append("fare_amount", 5.0).append("passenger_count", 2L)
                    .append("payment_type", "Cash"));
        }
    }

    @Test
    @Order(1)
    void test_import_parquet() {
        given()
                .multiPart("file", parquetFile)
                .when().post("/api/tables/" + TABLE + "/import")
                .then()
                .statusCode(201)
                .body("inserted", equalTo(5));
    }

    @Test
    @Order(2)
    void test_select_all_rows() {
        given()
                .when().get("/api/tables/" + TABLE + "/rows?limit=10")
                .then()
                .statusCode(200)
                .body("size()", is(5));
    }

    @Test
    @Order(3)
    void test_where_long_equal() {
        // WHERE vendor_id = 1 → 3 lignes
        given()
                .when().get("/api/tables/" + TABLE + "/query?q=SELECT * WHERE vendor_id = 1")
                .then()
                .statusCode(200)
                .body("size()", is(3));
    }

    @Test
    @Order(4)
    void test_where_double_greater_than() {
        // WHERE fare_amount > 15 → 2 lignes (20.0 et 35.0)
        given()
                .when().get("/api/tables/" + TABLE + "/query?q=SELECT * WHERE fare_amount > 15")
                .then()
                .statusCode(200)
                .body("size()", is(2));
    }

    @Test
    @Order(5)
    void test_where_string_equal() {
        // WHERE payment_type = 'Credit' → 3 lignes
        given()
                .when().get("/api/tables/" + TABLE + "/query?q=SELECT * WHERE payment_type = 'Credit'")
                .then()
                .statusCode(200)
                .body("size()", is(3));
    }

    @Test
    @Order(6)
    void test_groupby_count() {
        // GROUP BY payment_type → Credit(3) et Cash(2)
        given()
                .when().get("/api/tables/" + TABLE + "/group-by?column=payment_type")
                .then()
                .statusCode(200)
                .body("size()", is(2));
    }

    @Test
    @Order(7)
    void test_select_specific_columns_with_order() {
        // SELECT fare_amount, passenger_count ORDER BY fare_amount LIMIT 3
        given()
                .when().get("/api/tables/" + TABLE + "/query?q=SELECT fare_amount, passenger_count ORDER BY fare_amount LIMIT 3")
                .then()
                .statusCode(200)
                .body("size()", is(3))
                .body("[0][0]", equalTo(5.0f));  // fare_amount le plus bas = 5.0
    }
}
