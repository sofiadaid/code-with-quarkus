package org.acme.service;
import java.util.Collection;
import jakarta.enterprise.context.ApplicationScoped;
import org.acme.model.Table;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
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
}


