package org.acme.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.acme.model.Table;
import org.acme.model.Filter;

import java.util.*;

@ApplicationScoped
public class QueryExecutionService {

    @Inject
    TableRegistry registry;

    public List<List<Object>> select(String tableName, List<String> selectedColumns) {
        Table table = registry.get(tableName)
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        List<Integer> indexes = new ArrayList<>();

        for (String col : selectedColumns) {
            String normalizedCol = col.trim();
            Integer idx = table.getColIndex().get(normalizedCol);

            if (idx == null) {
                throw new IllegalArgumentException("Unknown column: " + col);
            }
            indexes.add(idx);
        }

        List<List<Object>> result = new ArrayList<>();

        for (Object[] row : table.getRows()) {
            List<Object> projectedRow = new ArrayList<>();

            for (int idx : indexes) {
                projectedRow.add(row[idx]);
            }

            result.add(projectedRow);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private boolean match(Object value, Filter filter) {
        if (value == null) return false;

        Comparable<Object> left = (Comparable<Object>) value;
        Object right = filter.getValue();

        return switch (filter.getOperator()) {
            case "=" -> left.compareTo(right) == 0;
            case ">" -> left.compareTo(right) > 0;
            case "<" -> left.compareTo(right) < 0;
            case ">=" -> left.compareTo(right) >= 0;
            case "<=" -> left.compareTo(right) <= 0;
            default -> throw new IllegalArgumentException("Unknown operator: " + filter.getOperator());
        };
    }

    public List<List<Object>> selectWhere(String tableName, List<String> selectedColumns, Filter filter) {

        Table table = registry.get(tableName)
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        List<Integer> indexes = new ArrayList<>();

        for (String col : selectedColumns) {
            Integer idx = table.getColIndex().get(col.trim());
            if (idx == null) {
                throw new IllegalArgumentException("Unknown column: " + col);
            }
            indexes.add(idx);
        }

        // index de la colonne WHERE
        Integer filterIndex = table.getColIndex().get(filter.getColumn());
        if (filterIndex == null) {
            throw new IllegalArgumentException("Unknown column in WHERE: " + filter.getColumn());
        }

        List<List<Object>> result = new ArrayList<>();

        for (Object[] row : table.getRows()) {

            Object value = row[filterIndex];

            if (match(value, filter)) {

                List<Object> projectedRow = new ArrayList<>();

                for (int idx : indexes) {
                    projectedRow.add(row[idx]);
                }

                result.add(projectedRow);
            }
        }

        return result;
    }

    public List<List<Object>> groupByCount(String tableName, String groupByColumn) {

        Table table = registry.get(tableName)
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        String normalizedGroupByColumn = groupByColumn.trim();
        Integer groupIndex = table.getColIndex().get(normalizedGroupByColumn);

        if (groupIndex == null) {
            throw new IllegalArgumentException("Unknown column for GROUP BY: " + groupByColumn);
        }

        Map<Object, Integer> counts = new LinkedHashMap<>();

        for (Object[] row : table.getRows()) {
            Object key = row[groupIndex];
            counts.put(key, counts.getOrDefault(key, 0) + 1);
        }

        List<List<Object>> result = new ArrayList<>();

        for (Map.Entry<Object, Integer> entry : counts.entrySet()) {
            List<Object> groupedRow = new ArrayList<>();
            groupedRow.add(entry.getKey());    // valeur de la colonne groupée
            groupedRow.add(entry.getValue());  // COUNT
            result.add(groupedRow);
        }

        return result;
    }

}