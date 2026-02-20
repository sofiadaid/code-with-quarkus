package org.acme.model;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class Table{
    public String name;
    public List<Columntable> columns;

    // Données
    public List<Object[]> rows = new ArrayList<>();

    // Index de colonnes : "age" -> 2
    public Map<String, Integer> colIndex = new HashMap<>();

    public Table(){}

    public Table(String name, List<Columntable> columns) {
        this.name = name;
        this.columns = columns;
        buildIndex();
    }

    public void buildIndex() {
        colIndex.clear();
        for (int i = 0; i < columns.size(); i++) {
            colIndex.put(columns.get(i).name, i);
        }
    }
}
