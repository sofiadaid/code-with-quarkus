package org.acme.fic_csv;

import org.acme.service.TableRegistry;

import java.io.*;
        import java.util.*;


public class CsvImporter {

    public static int importCsv(String tableName,
                                String path,
                                TableRegistry registry) throws Exception {


        File file = new File(path);

        if (!file.exists()) {
            throw new RuntimeException("Le fichier " + path + " n'existe pas !");
        }

        if (!file.canRead()) {
            throw new RuntimeException("Le fichier " + path + " n'est pas lisible !");
        }

        BufferedReader br = new BufferedReader(new FileReader(file));

        String header = br.readLine();
        if (header == null) {
            throw new RuntimeException("Fichier vide !");
        }

        String[] columnNames = header.split(",");

        List<Columntable> columns = new ArrayList<>();

        for (String colName : columnNames) {
            columns.add(new Columntable(colName.trim(), DataType.STRING));
            // On met String par defaut
        }

        Table table = new Table(tableName, columns);
        registry.create(table);

        List<List<Object>> rows = new ArrayList<>();
        String line;

        while ((line = br.readLine()) != null) {
            String[] values = line.split(",");
            rows.add(Arrays.asList(values));
        }

        br.close();

        return registry.insertRows(tableName, rows);
    }
}

/*
public class CsvImporter {
    public static int importCsv(String tableName, String path, TableRegistry registry) throws Exception {
        File file = new File(path);
        if (!file.exists()) {
            throw new RuntimeException("Le fichier " + path + " n'existe pas !");
        }
        if (!file.canRead()) {
            throw new RuntimeException("Le fichier " + path + " n'est pas lisible !");
        }
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
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
            return registry.insertRows(tableName, rows);
        }
    }
}
*/
