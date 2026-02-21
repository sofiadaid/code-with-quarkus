package org.acme.fic_csv;

import org.acme.service.TableRegistry;

import java.io.*;
        import java.util.*;

public class CsvImporter {

    public static int importCsv(String tableName, String path, TableRegistry registry) throws Exception {

        // Vérification du fichier
        File file = new File(path);

        if (!file.exists()) {
            throw new RuntimeException("Le fichier " + path + " n'existe pas !");
        }

        if (!file.canRead()) {
            throw new RuntimeException("Le fichier " + path + " n'est pas lisible !");
        }

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {

            // Ignorer le header (la table existe déjà)
            String header = br.readLine();
            if (header == null) {
                throw new RuntimeException("Fichier vide !");
            }

            List<List<Object>> rows = new ArrayList<>();
            String line;

            while ((line = br.readLine()) != null) {

                String[] values = line.split(",", -1);

                List<Object> row = new ArrayList<>();
                for (String value : values) {
                    row.add(value);
                }

                rows.add(row);
            }

            // 🔹 Ici on insère dans la table EXISTANTE
            return registry.insertRows(tableName, rows);
        }
    }
}