package org.acme.service;
import java.util.Collection;
import io.quarkus.runtime.util.StringUtil;
import jakarta.enterprise.context.ApplicationScoped;
import org.acme.model.Table;
import java.util.*;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.acme.model.DataType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.collections4.CollectionUtils;

@ApplicationScoped
public class TableRegistry {
    private final Map<String,Table> tables=new ConcurrentHashMap<>();

    public Table create(Table table) {
        if (table==null || StringUtils.isBlank(table.name)) {
            throw new IllegalArgumentException("Table name is required!!");
        }


        if (CollectionUtils.isEmpty(table.columns)) {
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


    private String normalize(String s){
        return s.trim();
    }

    public int insertRows(String tableName, List<List<Object>> inputRows) {
        Table t = get(tableName).orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        if (inputRows == null || inputRows.isEmpty()) {
            throw new IllegalArgumentException("No rows provided");
        }

        int expected = t.columns.size();

        int count = 0;
        for (List<Object> row : inputRows) {
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

    public List<List<Object>> getRows(String tableName, int offset, int limit) {
        Table t = get(tableName).orElseThrow(() ->
                new IllegalStateException("Table not found: " + tableName));

        if (offset < 0) offset = 0;
        if (limit <= 0) limit = 100;
        int end = Math.min(t.rows.size(), offset + limit);

        List<List<Object>> out = new java.util.ArrayList<>();
        for (int i = offset; i < end; i++) {
            Object[] row = t.rows.get(i);
            out.add(Arrays.asList(row));
        }
        return out;
    }

    public List<List<Object>> select(String tableName, List<String> selectedColumns) {

        Table table = get(tableName)
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        // Récupérer les index des colonnes demandées
        List<Integer> indexes = new ArrayList<>();

        for (String col : selectedColumns) {
            Integer idx = table.colIndex.get(col);
            if (idx == null) {
                throw new IllegalArgumentException("Unknown column: " + col);
            }
            indexes.add(idx);
        }

        List<List<Object>> result = new ArrayList<>();

        // Parcours des lignes
        for (Object[] row : table.rows) {

            List<Object> projectedRow = new ArrayList<>();

            for (int idx : indexes) {
                projectedRow.add(row[idx]);
            }

            result.add(projectedRow);
        }

        return result;
    }

    public List<List<Object>> selectQuery(String tableName, String query) {
        Table table = get(tableName)
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        // Parser la query : "SELECT col1, col2 WHERE col3 > 10 ORDER BY col1 LIMIT 50"
        String upper = query.trim().toUpperCase();

        if (!upper.startsWith("SELECT")) {
            throw new IllegalArgumentException("Query must start with SELECT");
        }

        // --- Extraire les parties ---
        String afterSelect = query.trim().substring(6).trim(); // tout après SELECT

        String selectPart, wherePart = null, orderByPart = null;
        int limitValue = -1;

        // LIMIT
        int limitIdx = upperIndexOf(afterSelect, "LIMIT");
        if (limitIdx != -1) {
            limitValue = Integer.parseInt(afterSelect.substring(limitIdx + 5).trim().split("\\s+")[0]);
            afterSelect = afterSelect.substring(0, limitIdx).trim();
        }

        // ORDER BY
        int orderIdx = upperIndexOf(afterSelect, "ORDER BY");
        if (orderIdx != -1) {
            orderByPart = afterSelect.substring(orderIdx + 8).trim();
            afterSelect = afterSelect.substring(0, orderIdx).trim();
        }

        // WHERE
        int whereIdx = upperIndexOf(afterSelect, "WHERE");
        if (whereIdx != -1) {
            wherePart = afterSelect.substring(whereIdx + 5).trim();
            afterSelect = afterSelect.substring(0, whereIdx).trim();
        }

        selectPart = afterSelect.trim();

        // --- Colonnes SELECT ---
        List<Integer> selectedIndexes = new ArrayList<>();
        List<String> selectedNames = new ArrayList<>();

        if (selectPart.equals("*")) {
            for (int i = 0; i < table.columns.size(); i++) {
                selectedIndexes.add(i);
                selectedNames.add(table.columns.get(i).name);
            }
        } else {
            for (String col : selectPart.split(",")) {
                String colName = col.trim();
                Integer idx = table.colIndex.get(colName);
                if (idx == null) throw new IllegalArgumentException("Unknown column: " + colName);
                selectedIndexes.add(idx);
                selectedNames.add(colName);
            }
        }

        // --- Filtrer (WHERE) ---
        List<Object[]> filtered = new ArrayList<>();
        for (Object[] row : table.rows) {
            if (wherePart == null || matchesWhere(row, wherePart, table)) {
                filtered.add(row);
            }
        }

        // --- Trier (ORDER BY) ---
        if (orderByPart != null) {
            String[] orderTokens = orderByPart.split("\\s+");
            String orderCol = orderTokens[0].trim();
            boolean desc = orderTokens.length > 1 && orderTokens[1].equalsIgnoreCase("DESC");
            Integer orderIdx2 = table.colIndex.get(orderCol);
            if (orderIdx2 == null) throw new IllegalArgumentException("Unknown column in ORDER BY: " + orderCol);

            filtered.sort((a, b) -> {
                Object va = a[orderIdx2];
                Object vb = b[orderIdx2];
                if (va == null && vb == null) return 0;
                if (va == null) return desc ? 1 : -1;
                if (vb == null) return desc ? -1 : 1;
                int cmp = ((Comparable) va).compareTo(vb);
                return desc ? -cmp : cmp;
            });
        }

        // --- LIMIT ---
        if (limitValue > 0 && filtered.size() > limitValue) {
            filtered = filtered.subList(0, limitValue);
        }

        // --- Projection ---
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

    // Cherche un mot-clé dans la query sans casser la casse des données
    private int upperIndexOf(String s, String keyword) {
        return s.toUpperCase().indexOf(keyword);
    }

    // Évalue une condition WHERE simple : "col OPERATEUR valeur"
    private boolean matchesWhere(Object[] row, String condition, Table table) {
        // Opérateurs supportés : >=, <=, !=, >, <, =, LIKE
        String[] ops = {">=", "<=", "!=", ">", "<", "="};

        for (String op : ops) {
            int opIdx = condition.indexOf(op);
            if (opIdx == -1) continue;

            String colName = condition.substring(0, opIdx).trim();
            String rawValue = condition.substring(opIdx + op.length()).trim()
                    .replaceAll("^'|'$", ""); // enlève les quotes si STRING

            Integer colIdx = table.colIndex.get(colName);
            if (colIdx == null) throw new IllegalArgumentException("Unknown column in WHERE: " + colName);

            Object cellValue = row[colIdx];
            if (cellValue == null) return false;

            // Comparaison selon le type
            try {
                if (cellValue instanceof Integer) {
                    int a = (Integer) cellValue, b = Integer.parseInt(rawValue);
                    return switch (op) {
                        case ">"  -> a > b;  case ">=" -> a >= b;
                        case "<"  -> a < b;  case "<=" -> a <= b;
                        case "="  -> a == b; case "!=" -> a != b;
                        default -> false;
                    };
                } else if (cellValue instanceof Long) {
                    long a = (Long) cellValue, b = Long.parseLong(rawValue);
                    return switch (op) {
                        case ">"  -> a > b;  case ">=" -> a >= b;
                        case "<"  -> a < b;  case "<=" -> a <= b;
                        case "="  -> a == b; case "!=" -> a != b;
                        default -> false;
                    };
                } else if (cellValue instanceof Double) {
                    double a = (Double) cellValue, b = Double.parseDouble(rawValue);
                    return switch (op) {
                        case ">"  -> a > b;  case ">=" -> a >= b;
                        case "<"  -> a < b;  case "<=" -> a <= b;
                        case "="  -> a == b; case "!=" -> a != b;
                        default -> false;
                    };
                } else { // STRING
                    int cmp = cellValue.toString().compareTo(rawValue);
                    return switch (op) {
                        case "="  -> cmp == 0; case "!=" -> cmp != 0;
                        case ">"  -> cmp > 0;  case ">=" -> cmp >= 0;
                        case "<"  -> cmp < 0;  case "<=" -> cmp <= 0;
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
            String colName = parts[0].trim();
            String pattern = parts[1].trim().replaceAll("^'|'$", "")
                    .replace("%", ".*").replace("_", ".");
            Integer colIdx = table.colIndex.get(colName);
            if (colIdx == null) throw new IllegalArgumentException("Unknown column in LIKE: " + colName);
            Object cellValue = row[colIdx];
            return cellValue != null && cellValue.toString().matches(pattern);
        }

        throw new IllegalArgumentException("Cannot parse WHERE condition: " + condition);
    }


    public Table createEmpty(String name) {
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("Table name is required");
        }
        String normalized = normalize(name);
        Table table = new Table();
        table.name = normalized;
        table.columns = new ArrayList<>();
        table.colIndex = new HashMap<>();
        Table previous = tables.putIfAbsent(normalized, table);
        if (previous != null) {
            throw new IllegalStateException("Table already exists: " + normalized);
        }
        return table;
    }




}



