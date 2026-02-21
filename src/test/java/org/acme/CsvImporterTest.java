package org.acme;

import org.acme.model.Columntable;
import org.acme.model.DataType;
import org.acme.model.Table;
import org.acme.service.TableRegistry;
import org.junit.jupiter.api.Test;
import org.acme.fic_csv.CsvImporter;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CsvImporterTest {

    @Test
    void testImportCsv() throws Exception {

        // 🔹 1️⃣ Créer un fichier CSV temporaire
        File tempFile = File.createTempFile("test", ".csv");

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("id,name\n");
            writer.write("1,Ania\n");
            writer.write("2,Sofia\n");
        }

        // 🔹 2️⃣ Créer la table AVANT l'import
        TableRegistry registry = new TableRegistry();

        List<Columntable> columns = List.of(
                new Columntable("id", DataType.STRING),
                new Columntable("name", DataType.STRING)
        );

        Table table = new Table("TestTable", columns);
        registry.create(table);

        // 🔹 3️⃣ Importer le CSV
        int inserted = CsvImporter.importCsv(
                "TestTable",
                tempFile.getAbsolutePath(),
                registry
        );

        // 🔹 4️⃣ Vérifications

        assertEquals(2, inserted);

        Table result = registry.get("TestTable").orElseThrow();

        assertEquals(2, result.rows.size());

        assertEquals("1", result.rows.get(0)[0]);
        assertEquals("Ania", result.rows.get(0)[1]);

        assertEquals("2", result.rows.get(1)[0]);
        assertEquals("Lina", result.rows.get(1)[1]);

        tempFile.delete();
    }
}