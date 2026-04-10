package org.acme.service;

import jakarta.enterprise.context.ApplicationScoped;
import org.acme.model.Table;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.acme.model.DataType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.collections4.CollectionUtils;

@ApplicationScoped
public class TableRegistry {
    private final Map<String,Table> tables=new ConcurrentHashMap<>();

    public Table create(Table table) {
        if (table==null || StringUtils.isBlank(table.getName())) {
            throw new IllegalArgumentException("Table name is required!!");
        }


        if (CollectionUtils.isEmpty(table.getColumns())) {
            throw new IllegalArgumentException("At least one column is required");
        }

        table.buildIndex();

        String name = normalize(table.getName());
        //putIfAbsent->no data race
        table.setName(name);
        Table previous = tables.putIfAbsent(name, table);
        if (previous !=null) {
            throw new IllegalStateException("Table already exists: " +name);
        }
        return table;
    }

    //return the table if it exits
    public Optional<Table> get(String name) {
        if (name==null){
            return Optional.empty();}
        return Optional.ofNullable(tables.get(normalize(name)));
    }


    private String normalize(String s){
        return s.trim();
    }

    public int insertRows(String tableName, List<List<Object>> inputRows) {
        Table t = get(tableName).orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        if (inputRows == null || inputRows.isEmpty()) {
            throw new IllegalArgumentException("No rows provided");
        }

        int expected = t.getColumns().size();

        int count = 0;
        for (List<Object> row : inputRows) {
            if (row.size() != expected) {
                throw new IllegalArgumentException("Row size mismatch. Expected " + expected + " values, got " + row.size());
            }

            Object[] converted = new Object[expected];
            for (int i = 0; i < expected; i++) {
                DataType type = t.getColumns().get(i).getType();
                converted[i] = convert(row.get(i), type, t.getColumns().get(i).getName());
            }

            t.getRows().add(converted);
            count++;
        }
        return count;
    }

    private Object convert(Object value, DataType type, String colName) {
        if (value == null) return null;

        try {
            return switch (type) {
                case INT -> (value instanceof Number) ? ((Number) value).intValue() : Integer.parseInt(value.toString());
                case LONG -> (value instanceof Number) ? ((Number) value).longValue() : Long.parseLong(value.toString());
                case DOUBLE -> (value instanceof Number) ? ((Number) value).doubleValue() : Double.parseDouble(value.toString());
                case STRING -> value.toString();
                case DATE -> java.time.LocalDate.parse(value.toString()); // format: YYYY-MM-DD
            };
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid value for column '" + colName + "' (" + type + "): " + value);
        }
    }

    public List<List<Object>> getRows(String tableName, int offset, int limit) {
        Table t = get(tableName).orElseThrow(() ->
                new IllegalStateException("Table not found: " + tableName));

        if (offset < 0) offset = 0;
        if (limit <= 0) limit = 100;
        int end = Math.min(t.getRows().size(), offset + limit);

        List<List<Object>> out = new java.util.ArrayList<>();
        for (int i = offset; i < end; i++) {
            Object[] row = t.getRows().get(i);
            out.add(Arrays.asList(row));
        }
        return out;
    }


}



