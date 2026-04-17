package org.acme.model;

import java.util.*;

public class Table {

    private String name;
    private List<Column> columns;

    private Map<String, Integer> colIndex = new HashMap<>();
    private LinkedHashMap<String, List<Object>> data = new LinkedHashMap<>();

    // colonne -> (valeur -> liste des rowIndex)
    private Map<String, Map<Object, List<Integer>>> indexes = new HashMap<>();

    // colonnes qu'on veut indexer
    private Set<String> indexedColumns = new HashSet<>();

    public Table() {
    }

    public Table(String name, List<Column> columns) {
        this.name = name;
        this.columns = columns;
        buildIndex();
        initializeStorage();
        initializeIndexes();
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

    public void initializeIndexes() {
        indexes.clear();

        for (String columnName : indexedColumns) {
            indexes.put(columnName, new HashMap<>());
        }
    }

    public void addIndexedColumn(String columnName) {
        if (columnName == null || columnName.isBlank()) {
            return;
        }

        String trimmed = columnName.trim();
        indexedColumns.add(trimmed);

        if (!indexes.containsKey(trimmed)) {
            indexes.put(trimmed, new HashMap<>());
        }

        // si des données existent déjà, reconstruire l'index de cette colonne
        if (data.containsKey(trimmed)) {
            rebuildIndexForColumn(trimmed);
        }
    }

    public void rebuildAllIndexes() {
        initializeIndexes();

        for (String columnName : indexedColumns) {
            rebuildIndexForColumn(columnName);
        }
    }

    private void rebuildIndexForColumn(String columnName) {
        if (!data.containsKey(columnName)) {
            return;
        }

        Map<Object, List<Integer>> columnIndex = indexes.computeIfAbsent(columnName, k -> new HashMap<>());
        columnIndex.clear();

        List<Object> columnData = data.get(columnName);
        for (int rowIndex = 0; rowIndex < columnData.size(); rowIndex++) {
            Object value = columnData.get(rowIndex);
            columnIndex.computeIfAbsent(value, k -> new ArrayList<>()).add(rowIndex);
        }
    }

    public void addRow(Object[] row) {
        if (row == null || columns == null || row.length != columns.size()) {
            throw new IllegalArgumentException("Row size does not match table schema.");
        }

        int rowIndex = rowCount();

        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i).getName();
            Object value = row[i];

            data.get(columnName).add(value);

            if (indexedColumns.contains(columnName)) {
                indexes
                        .computeIfAbsent(columnName, k -> new HashMap<>())
                        .computeIfAbsent(value, k -> new ArrayList<>())
                        .add(rowIndex);
            }
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

    public boolean isIndexed(String columnName) {
        return indexedColumns.contains(columnName);
    }

    public Map<Object, List<Integer>> getIndexForColumn(String columnName) {
        return indexes.get(columnName);
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
        initializeIndexes();
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

    public Map<String, Map<Object, List<Integer>>> getIndexes() {
        return indexes;
    }

    public void setIndexes(Map<String, Map<Object, List<Integer>>> indexes) {
        this.indexes = indexes;
    }

    public Set<String> getIndexedColumns() {
        return indexedColumns;
    }

    public void setIndexedColumns(Set<String> indexedColumns) {
        this.indexedColumns = indexedColumns != null ? indexedColumns : new HashSet<>();
        initializeIndexes();
    }
}