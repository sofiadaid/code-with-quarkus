package org.acme.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.acme.model.Filter;
import org.acme.model.Table;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class QueryExecutionService {

    @Inject
    TableRegistry registry;

    public List<List<Object>> selectQuery(String tableName, String query) {
        Table table = registry.get(tableName)
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        String trimmedQuery = query.trim();
        String upper = trimmedQuery.toUpperCase();

        if (!upper.startsWith("SELECT")) {
            throw new IllegalArgumentException("Query must start with SELECT");
        }

        String afterSelect = trimmedQuery.substring(6).trim();

        String selectPart;
        String wherePart = null;
        String orderByPart = null;
        int limitValue = -1;

        int limitIdx = upperIndexOf(afterSelect, "LIMIT");
        if (limitIdx != -1) {
            limitValue = Integer.parseInt(afterSelect.substring(limitIdx + 5).trim().split("\\s+")[0]);
            afterSelect = afterSelect.substring(0, limitIdx).trim();
        }

        int orderIdx = upperIndexOf(afterSelect, "ORDER BY");
        if (orderIdx != -1) {
            orderByPart = afterSelect.substring(orderIdx + 8).trim();
            afterSelect = afterSelect.substring(0, orderIdx).trim();
        }

        int whereIdx = upperIndexOf(afterSelect, "WHERE");
        if (whereIdx != -1) {
            wherePart = afterSelect.substring(whereIdx + 5).trim();
            afterSelect = afterSelect.substring(0, whereIdx).trim();
        }

        selectPart = afterSelect.trim();

        List<String> selectedColumns;
        if (selectPart.equals("*")) {
            selectedColumns = getAllColumnNames(table);
        } else {
            selectedColumns = new ArrayList<>();
            for (String col : selectPart.split(",")) {
                String colName = col.trim();
                if (!table.getData().containsKey(colName)) {
                    throw new IllegalArgumentException("Unknown column: " + colName);
                }
                selectedColumns.add(colName);
            }
        }

        List<Integer> matchingRowIndexes = new ArrayList<>();
        int rowCount = table.rowCount();

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            if (wherePart == null || matchesWhere(table, rowIndex, wherePart)) {
                matchingRowIndexes.add(rowIndex);
            }
        }

        if (orderByPart != null) {
            String[] tokens = orderByPart.split("\\s+");
            String orderCol = tokens[0].trim();
            boolean desc = tokens.length > 1 && tokens[1].equalsIgnoreCase("DESC");

            if (!table.getData().containsKey(orderCol)) {
                throw new IllegalArgumentException("Unknown column in ORDER BY: " + orderCol);
            }

            matchingRowIndexes.sort((a, b) -> {
                Object va = table.getData().get(orderCol).get(a);
                Object vb = table.getData().get(orderCol).get(b);

                if (va == null && vb == null) return 0;
                if (va == null) return desc ? 1 : -1;
                if (vb == null) return desc ? -1 : 1;

                @SuppressWarnings("unchecked")
                int cmp = ((Comparable<Object>) va).compareTo(vb);
                return desc ? -cmp : cmp;
            });
        }

        if (limitValue > 0 && matchingRowIndexes.size() > limitValue) {
            matchingRowIndexes = matchingRowIndexes.subList(0, limitValue);
        }

        List<List<Object>> result = new ArrayList<>();
        for (int rowIndex : matchingRowIndexes) {
            result.add(buildRow(table, rowIndex, selectedColumns));
        }

        return result;
    }

    public List<List<Object>> select(String tableName, List<String> selectedColumns) {
        Table table = registry.get(tableName)
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        if (selectedColumns == null || selectedColumns.isEmpty()) {
            throw new IllegalArgumentException("At least one column must be selected");
        }

        for (String col : selectedColumns) {
            if (!table.getData().containsKey(col.trim())) {
                throw new IllegalArgumentException("Unknown column: " + col);
            }
        }

        int rowCount = table.rowCount();
        List<List<Object>> result = new ArrayList<>();

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            result.add(buildRow(table, rowIndex, selectedColumns));
        }

        return result;
    }

    public List<List<Object>> selectWhere(String tableName, List<String> selectedColumns, Filter filter) {
        Table table = registry.get(tableName)
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        if (selectedColumns == null || selectedColumns.isEmpty()) {
            throw new IllegalArgumentException("At least one column must be selected");
        }

        for (String col : selectedColumns) {
            if (!table.getData().containsKey(col.trim())) {
                throw new IllegalArgumentException("Unknown column: " + col);
            }
        }

        if (!table.getData().containsKey(filter.getColumn())) {
            throw new IllegalArgumentException("Unknown column in WHERE: " + filter.getColumn());
        }

        int rowCount = table.rowCount();
        List<List<Object>> result = new ArrayList<>();

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            Object value = table.getData().get(filter.getColumn()).get(rowIndex);
            if (matchFilter(value, filter)) {
                result.add(buildRow(table, rowIndex, selectedColumns));
            }
        }

        return result;
    }

    public List<List<Object>> groupByCount(String tableName, String groupByColumn) {
        Table table = registry.get(tableName)
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        String columnName = groupByColumn.trim();
        if (!table.getData().containsKey(columnName)) {
            throw new IllegalArgumentException("Unknown column for GROUP BY: " + groupByColumn);
        }

        Map<Object, Integer> counts = new LinkedHashMap<>();
        List<Object> values = table.getData().get(columnName);

        for (Object value : values) {
            counts.put(value, counts.getOrDefault(value, 0) + 1);
        }

        List<List<Object>> result = new ArrayList<>();
        for (Map.Entry<Object, Integer> entry : counts.entrySet()) {
            result.add(List.of(entry.getKey(), entry.getValue()));
        }

        return result;
    }

    private boolean matchesWhere(Table table, int rowIndex, String condition) {
        String[] ops = {">=", "<=", "!=", ">", "<", "="};

        for (String op : ops) {
            int opIdx = condition.indexOf(op);
            if (opIdx == -1) {
                continue;
            }

            String colName = condition.substring(0, opIdx).trim();
            String rawValue = condition.substring(opIdx + op.length()).trim().replaceAll("^'|'$", "");

            if (!table.getData().containsKey(colName)) {
                throw new IllegalArgumentException("Unknown column in WHERE: " + colName);
            }

            Object cell = table.getData().get(colName).get(rowIndex);
            if (cell == null) {
                return false;
            }

            try {
                if (cell instanceof Integer) {
                    int a = (Integer) cell;
                    int b = Integer.parseInt(rawValue);
                    return switch (op) {
                        case "=" -> a == b;
                        case "!=" -> a != b;
                        case ">" -> a > b;
                        case ">=" -> a >= b;
                        case "<" -> a < b;
                        case "<=" -> a <= b;
                        default -> false;
                    };
                } else if (cell instanceof Long) {
                    long a = (Long) cell;
                    long b = Long.parseLong(rawValue);
                    return switch (op) {
                        case "=" -> a == b;
                        case "!=" -> a != b;
                        case ">" -> a > b;
                        case ">=" -> a >= b;
                        case "<" -> a < b;
                        case "<=" -> a <= b;
                        default -> false;
                    };
                } else if (cell instanceof Double) {
                    double a = (Double) cell;
                    double b = Double.parseDouble(rawValue);
                    return switch (op) {
                        case "=" -> a == b;
                        case "!=" -> a != b;
                        case ">" -> a > b;
                        case ">=" -> a >= b;
                        case "<" -> a < b;
                        case "<=" -> a <= b;
                        default -> false;
                    };
                } else {
                    int cmp = cell.toString().compareTo(rawValue);
                    return switch (op) {
                        case "=" -> cmp == 0;
                        case "!=" -> cmp != 0;
                        case ">" -> cmp > 0;
                        case ">=" -> cmp >= 0;
                        case "<" -> cmp < 0;
                        case "<=" -> cmp <= 0;
                        default -> false;
                    };
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid value in WHERE: " + rawValue);
            }
        }

        if (condition.toUpperCase().contains(" LIKE ")) {
            String[] parts = condition.split("(?i)\\sLIKE\\s");
            String colName = parts[0].trim();
            String pattern = parts[1].trim()
                    .replaceAll("^'|'$", "")
                    .replace("%", ".*")
                    .replace("_", ".");

            if (!table.getData().containsKey(colName)) {
                throw new IllegalArgumentException("Unknown column in LIKE: " + colName);
            }

            Object cell = table.getData().get(colName).get(rowIndex);
            return cell != null && cell.toString().matches(pattern);
        }

        throw new IllegalArgumentException("Cannot parse WHERE condition: " + condition);
    }

    @SuppressWarnings("unchecked")
    private boolean matchFilter(Object value, Filter filter) {
        if (value == null) {
            return false;
        }

        Comparable<Object> left = (Comparable<Object>) value;
        Object right = filter.getValue();

        return switch (filter.getOperator()) {
            case "=" -> left.compareTo(right) == 0;
            case "!=" -> left.compareTo(right) != 0;
            case ">" -> left.compareTo(right) > 0;
            case ">=" -> left.compareTo(right) >= 0;
            case "<" -> left.compareTo(right) < 0;
            case "<=" -> left.compareTo(right) <= 0;
            default -> throw new IllegalArgumentException("Unknown operator: " + filter.getOperator());
        };
    }

    private int upperIndexOf(String s, String keyword) {
        return s.toUpperCase().indexOf(keyword);
    }

    private List<String> getAllColumnNames(Table table) {
        return new ArrayList<>(table.getData().keySet());
    }

    private List<Object> buildRow(Table table, int rowIndex, List<String> selectedColumns) {
        List<Object> row = new ArrayList<>();

        for (String columnName : selectedColumns) {
            String trimmedName = columnName.trim();
            row.add(table.getData().get(trimmedName).get(rowIndex));
        }

        return row;
    }
}