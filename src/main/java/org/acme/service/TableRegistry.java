package org.acme.service;
import java.util.Collection;
import jakarta.enterprise.context.ApplicationScoped;
import org.acme.model.Table;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.acme.model.DataType;

@ApplicationScoped
public class TableRegistry {
    private final Map<String,Table> tables=new ConcurrentHashMap<>();

    public Table create(Table table) {
        if (table==null || table.name ==null || table.name.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name is required!!");
        }
        if (table.columns== null || table.columns.isEmpty()) {
            throw new IllegalArgumentException("At least one column is required");
        }
        String name = normalize(table.name);
        //putIfAbsent->no data race
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

    public Collection<Table> list() {
        return tables.values();
    }

    public boolean drop(String name) {
        if (name==null) return false;
        return tables.remove(normalize(name)) !=null;
    }
    public boolean exists(String name) {
        if (name==null) return false;
        return tables.containsKey(normalize(name));
    }

    private String normalize(String s){
        return s.trim();
    }

    public int insertRows(String tableName, java.util.List<java.util.List<Object>> inputRows) {
        Table t = get(tableName).orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        if (inputRows == null || inputRows.isEmpty()) {
            throw new IllegalArgumentException("No rows provided");
        }

        int expected = t.columns.size();

        int count = 0;
        for (java.util.List<Object> row : inputRows) {
            if (row.size() != expected) {
                throw new IllegalArgumentException("Row size mismatch. Expected " + expected + " values, got " + row.size());
            }

            Object[] converted = new Object[expected];
            for (int i = 0; i < expected; i++) {
                DataType type = t.columns.get(i).type;
                converted[i] = convert(row.get(i), type, t.columns.get(i).name);
            }

            t.rows.add(converted);
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

}



