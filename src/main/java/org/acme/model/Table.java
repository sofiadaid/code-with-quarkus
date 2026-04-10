package org.acme.model;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class Table{
    private String name;
    private List<Column> columns;

    // Données
    private List<Object[]> rows = new ArrayList<>();

    // Index de colonnes : "age" -> 2
    private Map<String, Integer> colIndex = new HashMap<>();

    public Table(){}

    public Table(String name, List<Column> columns) {
        this.name = name;
        this.columns = columns;
        buildIndex();
    }

    public void buildIndex() {
        colIndex.clear();
        for (int i = 0; i < columns.size(); i++) {
            colIndex.put(columns.get(i).getName(), i);
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public List<Object[]> getRows() {
        return rows;
    }

    public void setRows(List<Object[]> rows) {
        this.rows = rows;
    }

    public Map<String, Integer> getColIndex() {
        return colIndex;
    }

    public void setColIndex(Map<String, Integer> colIndex) {
        this.colIndex = colIndex;
    }
}
