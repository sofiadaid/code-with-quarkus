package org.acme.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Table {

    private String name;
    private List<Column> columns;

    private Map<String, Integer> colIndex = new HashMap<>();
    private LinkedHashMap<String, List<Object>> data = new LinkedHashMap<>();

    public Table() {
    }

    public Table(String name, List<Column> columns) {
        this.name = name;
        this.columns = columns;
        buildIndex();
        initializeStorage();
    }

    public void buildIndex() {
        colIndex.clear();

        if (columns == null) {
            return;
        }

        for (int i = 0; i < columns.size(); i++) {
            colIndex.put(columns.get(i).getName(), i);
        }
    }

    public void initializeStorage() {
        data.clear();

        if (columns == null) {
            return;
        }

        for (Column column : columns) {
            data.put(column.getName(), new ArrayList<>());
        }
    }

    public void addRow(Object[] row) {
        if (row == null || columns == null || row.length != columns.size()) {
            throw new IllegalArgumentException("Row size does not match table schema.");
        }

        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i).getName();
            data.get(columnName).add(row[i]);
        }
    }

    public int rowCount() {
        if (columns == null || columns.isEmpty()) {
            return 0;
        }

        String firstColumnName = columns.get(0).getName();
        List<Object> firstColumnData = data.get(firstColumnName);

        return firstColumnData == null ? 0 : firstColumnData.size();
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
        buildIndex();
        initializeStorage();
    }

    public Map<String, Integer> getColIndex() {
        return colIndex;
    }

    public void setColIndex(Map<String, Integer> colIndex) {
        this.colIndex = colIndex;
    }

    public LinkedHashMap<String, List<Object>> getData() {
        return data;
    }

    public void setData(LinkedHashMap<String, List<Object>> data) {
        this.data = data;
    }
}