package org.acme.service;

import jakarta.enterprise.context.ApplicationScoped;
import org.acme.model.Column;
import org.acme.model.DataType;
import org.acme.model.Table;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registre central des tables — stockage in-memory thread-safe.
 * Responsabilités : créer, supprimer, récupérer les tables, insérer et lire les lignes.
 * La logique de requêtage est dans QueryExecutionService.
 */
@ApplicationScoped
public class TableRegistry {

    private final Map<String, Table> tables = new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // Gestion des tables
    // -------------------------------------------------------------------------

    public Table create(Table table) {
        if (table == null || StringUtils.isBlank(table.getName())) {
            throw new IllegalArgumentException("Table name is required");
        }
        if (CollectionUtils.isEmpty(table.getColumns())) {
            throw new IllegalArgumentException("At least one column is required");
        }

        String name = normalize(table.getName());
        table.setName(name);
        table.buildIndex();

        Table previous = tables.putIfAbsent(name, table);
        if (previous != null) {
            throw new IllegalStateException("Table already exists: " + name);
        }
        return table;
    }

    public Optional<Table> get(String name) {
        if (name == null) return Optional.empty();
        return Optional.ofNullable(tables.get(normalize(name)));
    }

    public void delete(String name) {
        if (name == null) return;
        tables.remove(normalize(name));
    }

    // -------------------------------------------------------------------------
    // Insertion et lecture des lignes
    // -------------------------------------------------------------------------

    public int insertRows(String tableName, List<List<Object>> inputRows) {
        Table t = get(tableName)
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        if (inputRows == null || inputRows.isEmpty()) {
            throw new IllegalArgumentException("No rows provided");
        }

        int expected = t.getColumns().size();
        int count = 0;

        for (List<Object> row : inputRows) {
            if (row.size() != expected) {
                throw new IllegalArgumentException(
                        "Row size mismatch. Expected " + expected + " values, got " + row.size());
            }

            Object[] converted = new Object[expected];
            for (int i = 0; i < expected; i++) {
                Column col = t.getColumns().get(i);
                converted[i] = convert(row.get(i), col.getType(), col.getName());
            }

            t.getRows().add(converted);
            count++;
        }
        return count;
    }

    public List<List<Object>> getRows(String tableName, int offset, int limit) {
        Table t = get(tableName)
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        if (offset < 0) offset = 0;
        if (limit <= 0) limit = 100;
        int end = Math.min(t.getRows().size(), offset + limit);

        List<List<Object>> out = new ArrayList<>();
        for (int i = offset; i < end; i++) {
            out.add(Arrays.asList(t.getRows().get(i)));
        }
        return out;
    }

    // -------------------------------------------------------------------------
    // Méthodes privées
    // -------------------------------------------------------------------------

    private String normalize(String s) {
        return s.trim();
    }

    private Object convert(Object value, DataType type, String colName) {
        if (value == null) return null;
        try {
            return switch (type) {
                case INT    -> (value instanceof Number n) ? n.intValue()    : Integer.parseInt(value.toString());
                case LONG   -> (value instanceof Number n) ? n.longValue()   : Long.parseLong(value.toString());
                case DOUBLE -> (value instanceof Number n) ? n.doubleValue() : Double.parseDouble(value.toString());
                case STRING -> value.toString();
                case DATE   -> java.time.LocalDate.parse(value.toString());
            };
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid value for column '" + colName + "' (" + type + "): " + value);
        }
    }
}
