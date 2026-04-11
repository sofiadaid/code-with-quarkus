package org.acme.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.acme.model.Filter;
import org.acme.model.Table;

import java.util.*;

/**
 * Service responsable de toute la logique de requêtage :
 * - Parser SQL (SELECT / WHERE / ORDER BY / LIMIT)
 * - Filtrage (WHERE avec opérateurs et LIKE)
 * - Agrégation (GROUP BY + COUNT)
 * - Projection (SELECT colonnes)
 */
@ApplicationScoped
public class QueryExecutionService {

    @Inject
    TableRegistry registry;

    // -------------------------------------------------------------------------
    // API principale — parser SQL complet
    // -------------------------------------------------------------------------

    /**
     * Exécute une requête SQL textuelle :
     * "SELECT col1, col2 WHERE col3 > 10 ORDER BY col1 DESC LIMIT 50"
     */
    public List<List<Object>> selectQuery(String tableName, String query) {
        Table table = registry.get(tableName)
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        String upper = query.trim().toUpperCase();
        if (!upper.startsWith("SELECT")) {
            throw new IllegalArgumentException("Query must start with SELECT");
        }

        String afterSelect = query.trim().substring(6).trim();

        String selectPart;
        String wherePart = null;
        String orderByPart = null;
        int limitValue = -1;

        // Extraire LIMIT
        int limitIdx = upperIndexOf(afterSelect, "LIMIT");
        if (limitIdx != -1) {
            limitValue = Integer.parseInt(afterSelect.substring(limitIdx + 5).trim().split("\\s+")[0]);
            afterSelect = afterSelect.substring(0, limitIdx).trim();
        }

        // Extraire ORDER BY
        int orderIdx = upperIndexOf(afterSelect, "ORDER BY");
        if (orderIdx != -1) {
            orderByPart = afterSelect.substring(orderIdx + 8).trim();
            afterSelect = afterSelect.substring(0, orderIdx).trim();
        }

        // Extraire WHERE
        int whereIdx = upperIndexOf(afterSelect, "WHERE");
        if (whereIdx != -1) {
            wherePart = afterSelect.substring(whereIdx + 5).trim();
            afterSelect = afterSelect.substring(0, whereIdx).trim();
        }

        selectPart = afterSelect.trim();

        // Colonnes à projeter
        List<Integer> selectedIndexes = new ArrayList<>();
        if (selectPart.equals("*")) {
            for (int i = 0; i < table.getColumns().size(); i++) {
                selectedIndexes.add(i);
            }
        } else {
            for (String col : selectPart.split(",")) {
                String colName = col.trim();
                Integer idx = table.getColIndex().get(colName);
                if (idx == null) throw new IllegalArgumentException("Unknown column: " + colName);
                selectedIndexes.add(idx);
            }
        }

        // Filtrage WHERE
        List<Object[]> filtered = new ArrayList<>();
        for (Object[] row : table.getRows()) {
            if (wherePart == null || matchesWhere(row, wherePart, table)) {
                filtered.add(row);
            }
        }

        // Tri ORDER BY
        if (orderByPart != null) {
            String[] tokens = orderByPart.split("\\s+");
            String orderCol = tokens[0].trim();
            boolean desc = tokens.length > 1 && tokens[1].equalsIgnoreCase("DESC");
            Integer orderColIdx = table.getColIndex().get(orderCol);
            if (orderColIdx == null) throw new IllegalArgumentException("Unknown column in ORDER BY: " + orderCol);

            filtered.sort((a, b) -> {
                Object va = a[orderColIdx];
                Object vb = b[orderColIdx];
                if (va == null && vb == null) return 0;
                if (va == null) return desc ? 1 : -1;
                if (vb == null) return desc ? -1 : 1;
                @SuppressWarnings("unchecked")
                int cmp = ((Comparable<Object>) va).compareTo(vb);
                return desc ? -cmp : cmp;
            });
        }

        // LIMIT
        if (limitValue > 0 && filtered.size() > limitValue) {
            filtered = filtered.subList(0, limitValue);
        }

        // Projection finale
        List<List<Object>> result = new ArrayList<>();
        for (Object[] row : filtered) {
            List<Object> projectedRow = new ArrayList<>();
            for (int idx : selectedIndexes) {
                projectedRow.add(row[idx]);
            }
            result.add(projectedRow);
        }

        return result;
    }

    // -------------------------------------------------------------------------
    // API structurée — utilisée par POST /select-where (body JSON avec Filter)
    // -------------------------------------------------------------------------

    /**
     * Projection simple sans filtre.
     */
    public List<List<Object>> select(String tableName, List<String> selectedColumns) {
        Table table = registry.get(tableName)
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        List<Integer> indexes = resolveColumns(table, selectedColumns);
        List<List<Object>> result = new ArrayList<>();
        for (Object[] row : table.getRows()) {
            result.add(project(row, indexes));
        }
        return result;
    }

    /**
     * Projection avec filtre structuré (objet Filter).
     */
    public List<List<Object>> selectWhere(String tableName, List<String> selectedColumns, Filter filter) {
        Table table = registry.get(tableName)
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        List<Integer> indexes = resolveColumns(table, selectedColumns);

        Integer filterIndex = table.getColIndex().get(filter.getColumn());
        if (filterIndex == null) {
            throw new IllegalArgumentException("Unknown column in WHERE: " + filter.getColumn());
        }

        List<List<Object>> result = new ArrayList<>();
        for (Object[] row : table.getRows()) {
            if (matchFilter(row[filterIndex], filter)) {
                result.add(project(row, indexes));
            }
        }
        return result;
    }

    /**
     * GROUP BY avec COUNT.
     */
    public List<List<Object>> groupByCount(String tableName, String groupByColumn) {
        Table table = registry.get(tableName)
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        Integer groupIndex = table.getColIndex().get(groupByColumn.trim());
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
            result.add(List.of(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // Méthodes privées
    // -------------------------------------------------------------------------

    /** Résout une liste de noms de colonnes en indices. */
    private List<Integer> resolveColumns(Table table, List<String> colNames) {
        List<Integer> indexes = new ArrayList<>();
        for (String col : colNames) {
            Integer idx = table.getColIndex().get(col.trim());
            if (idx == null) throw new IllegalArgumentException("Unknown column: " + col);
            indexes.add(idx);
        }
        return indexes;
    }

    /** Projette une ligne sur les indices donnés. */
    private List<Object> project(Object[] row, List<Integer> indexes) {
        List<Object> out = new ArrayList<>();
        for (int idx : indexes) out.add(row[idx]);
        return out;
    }

    /** Évalue une condition WHERE textuelle "col OP valeur" sur une ligne. */
    private boolean matchesWhere(Object[] row, String condition, Table table) {
        // Opérateurs multi-char en premier pour éviter que '>' match avant '>='
        String[] ops = {">=", "<=", "!=", ">", "<", "="};

        for (String op : ops) {
            int opIdx = condition.indexOf(op);
            if (opIdx == -1) continue;

            String colName  = condition.substring(0, opIdx).trim();
            String rawValue = condition.substring(opIdx + op.length()).trim()
                    .replaceAll("^'|'$", "");

            Integer colIdx = table.getColIndex().get(colName);
            if (colIdx == null) throw new IllegalArgumentException("Unknown column in WHERE: " + colName);

            Object cell = row[colIdx];
            if (cell == null) return false;

            try {
                if (cell instanceof Integer) {
                    int a = (Integer) cell, b = Integer.parseInt(rawValue);
                    return switch (op) {
                        case "=" -> a == b; case "!=" -> a != b;
                        case ">" -> a > b;  case ">=" -> a >= b;
                        case "<" -> a < b;  case "<=" -> a <= b;
                        default -> false;
                    };
                } else if (cell instanceof Long) {
                    long a = (Long) cell, b = Long.parseLong(rawValue);
                    return switch (op) {
                        case "=" -> a == b; case "!=" -> a != b;
                        case ">" -> a > b;  case ">=" -> a >= b;
                        case "<" -> a < b;  case "<=" -> a <= b;
                        default -> false;
                    };
                } else if (cell instanceof Double) {
                    double a = (Double) cell, b = Double.parseDouble(rawValue);
                    return switch (op) {
                        case "=" -> a == b; case "!=" -> a != b;
                        case ">" -> a > b;  case ">=" -> a >= b;
                        case "<" -> a < b;  case "<=" -> a <= b;
                        default -> false;
                    };
                } else {
                    int cmp = cell.toString().compareTo(rawValue);
                    return switch (op) {
                        case "=" -> cmp == 0; case "!=" -> cmp != 0;
                        case ">" -> cmp > 0;  case ">=" -> cmp >= 0;
                        case "<" -> cmp < 0;  case "<=" -> cmp <= 0;
                        default -> false;
                    };
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid value in WHERE: " + rawValue);
            }
        }

        // LIKE
        if (condition.toUpperCase().contains(" LIKE ")) {
            String[] parts = condition.split("(?i)\\sLIKE\\s");
            String colName  = parts[0].trim();
            String pattern  = parts[1].trim().replaceAll("^'|'$", "")
                    .replace("%", ".*").replace("_", ".");
            Integer colIdx = table.getColIndex().get(colName);
            if (colIdx == null) throw new IllegalArgumentException("Unknown column in LIKE: " + colName);
            Object cell = row[colIdx];
            return cell != null && cell.toString().matches(pattern);
        }

        throw new IllegalArgumentException("Cannot parse WHERE condition: " + condition);
    }

    /** Évalue un filtre structuré (objet Filter) sur une valeur. */
    @SuppressWarnings("unchecked")
    private boolean matchFilter(Object value, Filter filter) {
        if (value == null) return false;
        Comparable<Object> left = (Comparable<Object>) value;
        Object right = filter.getValue();
        return switch (filter.getOperator()) {
            case "="  -> left.compareTo(right) == 0;
            case "!=" -> left.compareTo(right) != 0;
            case ">"  -> left.compareTo(right) > 0;
            case ">=" -> left.compareTo(right) >= 0;
            case "<"  -> left.compareTo(right) < 0;
            case "<=" -> left.compareTo(right) <= 0;
            default   -> throw new IllegalArgumentException("Unknown operator: " + filter.getOperator());
        };
    }

    /** indexOf insensible à la casse, sans modifier la chaîne originale. */
    private int upperIndexOf(String s, String keyword) {
        return s.toUpperCase().indexOf(keyword);
    }
}
