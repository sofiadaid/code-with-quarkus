package org.acme.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.acme.model.Filter;
import org.acme.model.Table;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Supporte les opérations : SELECT, WHERE, ORDER BY, LIMIT, GROUP BY, agrégations.
 */
@ApplicationScoped
public class QueryExecutionService {

    @Inject
    TableRegistry registry;

    /**
     * Exécute une requête SELECT textuelle sur une table.
     */
    public List<List<Object>> selectQuery(String tableName, String query) {
        Table table = getTable(tableName);

        if (query == null || query.isBlank()) {
            throw new IllegalArgumentException("Query is required");
        }

        String trimmedQuery = query.trim();
        String upperQuery = trimmedQuery.toUpperCase();

        if (!upperQuery.startsWith("SELECT")) {
            throw new IllegalArgumentException("Query must start with SELECT");
        }

        // Supprime le mot-clé SELECT pour traiter le reste
        String afterSelect = trimmedQuery.substring(6).trim();

        String selectPart;
        String wherePart = null;
        String orderByPart = null;
        int limitValue = -1;

        // Extraction de la clause LIMIT (en dernier pour ne pas perturber les autres)
        int limitIdx = upperIndexOf(afterSelect, "LIMIT");
        if (limitIdx != -1) {
            String limitPart = afterSelect.substring(limitIdx + 5).trim();
            limitValue = Integer.parseInt(limitPart.split("\\s+")[0]);
            afterSelect = afterSelect.substring(0, limitIdx).trim();
        }

        // Extraction de la clause ORDER BY
        int orderIdx = upperIndexOf(afterSelect, "ORDER BY");
        if (orderIdx != -1) {
            orderByPart = afterSelect.substring(orderIdx + 8).trim();
            afterSelect = afterSelect.substring(0, orderIdx).trim();
        }

        // Extraction de la clause WHERE
        int whereIdx = upperIndexOf(afterSelect, "WHERE");
        if (whereIdx != -1) {
            wherePart = afterSelect.substring(whereIdx + 5).trim();
            afterSelect = afterSelect.substring(0, whereIdx).trim();
        }

        // Ce qui reste est la partie SELECT (colonnes ou *)
        selectPart = afterSelect.trim();

        // Résolution des colonnes sélectionnées
        List<String> selectedColumns = resolveSelectedColumns(table, selectPart);

        // Collecte des index de lignes correspondant au WHERE
        List<Integer> matchingRowIndexes = collectMatchingRowIndexes(table, wherePart);

        // Application du tri si ORDER BY présent
        if (orderByPart != null && !orderByPart.isBlank()) {
            applyOrderBy(table, matchingRowIndexes, orderByPart);
        }

        // Application de la limite de résultats
        if (limitValue > 0 && matchingRowIndexes.size() > limitValue) {
            matchingRowIndexes = new ArrayList<>(matchingRowIndexes.subList(0, limitValue));
        }

        return buildRowsFromIndexes(table, matchingRowIndexes, selectedColumns);
    }

    /**
     * Retourne toutes les lignes d'une table pour les colonnes spécifiées.

     */
    public List<List<Object>> select(String tableName, List<String> selectedColumns) {
        Table table = getTable(tableName);

        if (selectedColumns == null || selectedColumns.isEmpty()) {
            throw new IllegalArgumentException("At least one column must be selected");
        }

        List<String> validatedColumns = validateColumns(table, selectedColumns);
        List<Integer> rowIndexes = allRowIndexes(table);

        return buildRowsFromIndexes(table, rowIndexes, validatedColumns);
    }

    /**
     * Retourne les lignes d'une table filtrées par un objet Filter, pour les colonnes spécifiées.
     */
    public List<List<Object>> selectWhere(String tableName, List<String> selectedColumns, Filter filter) {
        Table table = getTable(tableName);

        if (selectedColumns == null || selectedColumns.isEmpty()) {
            throw new IllegalArgumentException("At least one column must be selected");
        }

        if (filter == null) {
            throw new IllegalArgumentException("Filter is required");
        }

        List<String> validatedColumns = validateColumns(table, selectedColumns);

        String filterColumn = filter.getColumn();
        if (filterColumn == null || filterColumn.isBlank()) {
            throw new IllegalArgumentException("Filter column is required");
        }

        if (!table.getData().containsKey(filterColumn.trim())) {
            throw new IllegalArgumentException("Unknown column in WHERE: " + filterColumn);
        }

        // Parcours de toutes les lignes pour appliquer le filtre
        List<Integer> matchingRowIndexes = new ArrayList<>();
        int rowCount = table.rowCount();

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            Object value = getCellValue(table, filterColumn.trim(), rowIndex);
            if (matchFilter(value, filter)) {
                matchingRowIndexes.add(rowIndex);
            }
        }

        return buildRowsFromIndexes(table, matchingRowIndexes, validatedColumns);
    }

    /**
     * Compte le nombre de valeurs non nulles dans une colonne (ou toutes les lignes si colonne nulle).

     */
    public long count(String tableName, String column) {
        Table table = getTable(tableName);

        if (column == null || column.isBlank()) {
            return table.rowCount();
        }

        String col = column.trim();
        List<Object> values = getColumnValues(table, col);

        long count = 0;
        for (Object value : values) {
            if (value != null) {
                count++;
            }
        }

        return count;
    }

    /**
     * Retourne la valeur minimale d'une colonne.
     */
    public Object min(String tableName, String column) {
        Table table = getTable(tableName);
        List<Object> values = getColumnValues(table, column);

        Object min = null;
        for (Object value : values) {
            if (value == null) {
                continue;
            }
            if (min == null || compare(value, min) < 0) {
                min = value;
            }
        }

        return min;
    }

    /**
     * Retourne la valeur maximale d'une colonne.
     */
    public Object max(String tableName, String column) {
        Table table = getTable(tableName);
        List<Object> values = getColumnValues(table, column);

        Object max = null;
        for (Object value : values) {
            if (value == null) {
                continue;
            }
            if (max == null || compare(value, max) > 0) {
                max = value;
            }
        }

        return max;
    }

    /**
     * Retourne la somme des valeurs d'une colonne numérique.
     */
    public double sum(String tableName, String column) {
        Table table = getTable(tableName);
        List<Object> values = getColumnValues(table, column);

        double sum = 0.0;
        for (Object value : values) {
            if (value != null) {
                sum += toDouble(value, column);
            }
        }

        return sum;
    }

    /**
     * Retourne la moyenne des valeurs d'une colonne numérique.
     */
    public double avg(String tableName, String column) {
        Table table = getTable(tableName);
        List<Object> values = getColumnValues(table, column);

        double sum = 0.0;
        int count = 0;

        for (Object value : values) {
            if (value != null) {
                sum += toDouble(value, column);
                count++;
            }
        }

        if (count == 0) {
            return 0.0;
        }

        return sum / count;
    }

    /**
     * Groupe les lignes par une colonne et compte les occurrences de chaque valeur.
     */
    public List<List<Object>> groupByCount(String tableName, String groupByColumn) {
        Table table = getTable(tableName);
        List<Object> groupValues = getColumnValues(table, groupByColumn);

        Map<Object, Long> counts = new LinkedHashMap<>();

        for (Object key : groupValues) {
            counts.put(key, counts.getOrDefault(key, 0L) + 1L);
        }

        return buildGroupAggregateResult(counts);
    }

    /**
     * Groupe par une colonne et retourne le minimum d'une colonne cible par groupe.
     */
    public List<List<Object>> groupByMin(String tableName, String groupByColumn, String targetColumn) {
        Table table = getTable(tableName);
        List<Object> groupValues = getColumnValues(table, groupByColumn);
        List<Object> targetValues = getColumnValues(table, targetColumn);

        Map<Object, Object> mins = new LinkedHashMap<>();

        for (int i = 0; i < table.rowCount(); i++) {
            Object key = groupValues.get(i);
            Object value = targetValues.get(i);

            if (value == null) {
                continue;
            }

            if (!mins.containsKey(key) || compare(value, mins.get(key)) < 0) {
                mins.put(key, value);
            }
        }

        return buildGroupAggregateResult(mins);
    }

    /**
     * Groupe par une colonne et retourne le maximum d'une colonne cible par groupe.
     */
    public List<List<Object>> groupByMax(String tableName, String groupByColumn, String targetColumn) {
        Table table = getTable(tableName);
        List<Object> groupValues = getColumnValues(table, groupByColumn);
        List<Object> targetValues = getColumnValues(table, targetColumn);

        Map<Object, Object> maxs = new LinkedHashMap<>();

        for (int i = 0; i < table.rowCount(); i++) {
            Object key = groupValues.get(i);
            Object value = targetValues.get(i);

            if (value == null) {
                continue;
            }

            if (!maxs.containsKey(key) || compare(value, maxs.get(key)) > 0) {
                maxs.put(key, value);
            }
        }

        return buildGroupAggregateResult(maxs);
    }

    /**
     * Groupe par une colonne et retourne la somme d'une colonne cible par groupe.
     */
    public List<List<Object>> groupBySum(String tableName, String groupByColumn, String targetColumn) {
        Table table = getTable(tableName);
        List<Object> groupValues = getColumnValues(table, groupByColumn);
        List<Object> targetValues = getColumnValues(table, targetColumn);

        Map<Object, Double> sums = new LinkedHashMap<>();

        for (int i = 0; i < table.rowCount(); i++) {
            Object key = groupValues.get(i);
            Object value = targetValues.get(i);

            if (value == null) {
                continue;
            }

            sums.put(key, sums.getOrDefault(key, 0.0) + toDouble(value, targetColumn));
        }

        return buildGroupAggregateResult(sums);
    }

    /**
     * Groupe par une colonne et retourne la moyenne d'une colonne cible par groupe.
     */
    public List<List<Object>> groupByAvg(String tableName, String groupByColumn, String targetColumn) {
        Table table = getTable(tableName);
        List<Object> groupValues = getColumnValues(table, groupByColumn);
        List<Object> targetValues = getColumnValues(table, targetColumn);

        Map<Object, Double> sums = new LinkedHashMap<>();
        Map<Object, Integer> counts = new LinkedHashMap<>();

        for (int i = 0; i < table.rowCount(); i++) {
            Object key = groupValues.get(i);
            Object value = targetValues.get(i);

            if (value == null) {
                continue;
            }

            sums.put(key, sums.getOrDefault(key, 0.0) + toDouble(value, targetColumn));
            counts.put(key, counts.getOrDefault(key, 0) + 1);
        }

        // Calcul de la moyenne à partir des sommes et des comptes
        Map<Object, Double> avgs = new LinkedHashMap<>();
        for (Object key : sums.keySet()) {
            avgs.put(key, sums.get(key) / counts.get(key));
        }

        return buildGroupAggregateResult(avgs);
    }

    // -------------------------------------------------------------------------
    // Méthodes privées utilitaires
    // -------------------------------------------------------------------------

    /**
     * Résout la liste des colonnes depuis la partie SELECT de la requête.
     * Retourne toutes les colonnes si selectPart vaut "*".
     */
    private List<String> resolveSelectedColumns(Table table, String selectPart) {
        if (selectPart == null || selectPart.isBlank()) {
            throw new IllegalArgumentException("SELECT columns are required");
        }

        if ("*".equals(selectPart.trim())) {
            return new ArrayList<>(table.getData().keySet());
        }

        List<String> selectedColumns = new ArrayList<>();
        for (String col : selectPart.split(",")) {
            String colName = col.trim();
            if (!table.getData().containsKey(colName)) {
                throw new IllegalArgumentException("Unknown column: " + colName);
            }
            selectedColumns.add(colName);
        }

        return selectedColumns;
    }

    /**
     * Vérifie que toutes les colonnes demandées existent dans la table.
     */
    private List<String> validateColumns(Table table, List<String> columns) {
        List<String> validated = new ArrayList<>();

        for (String col : columns) {
            String trimmed = col.trim();
            if (!table.getData().containsKey(trimmed)) {
                throw new IllegalArgumentException("Unknown column: " + col);
            }
            validated.add(trimmed);
        }

        return validated;
    }

    /**
     * Collecte les index des lignes correspondant à la clause WHERE.
     * Si wherePart est null ou vide, toutes les lignes sont retournées.
     */
    private List<Integer> collectMatchingRowIndexes(Table table, String wherePart) {
        List<Integer> rowIndexes = new ArrayList<>();
        int rowCount = table.rowCount();

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            if (wherePart == null || wherePart.isBlank() || matchesWhere(table, rowIndex, wherePart)) {
                rowIndexes.add(rowIndex);
            }
        }

        return rowIndexes;
    }

    /**
     * Trie les index de lignes selon la clause ORDER BY.
     * Supporte ASC (défaut) et DESC.
     */
    private void applyOrderBy(Table table, List<Integer> rowIndexes, String orderByPart) {
        String[] tokens = orderByPart.trim().split("\\s+");
        String orderColumn = tokens[0].trim();
        boolean desc = tokens.length > 1 && tokens[1].equalsIgnoreCase("DESC");

        if (!table.getData().containsKey(orderColumn)) {
            throw new IllegalArgumentException("Unknown column in ORDER BY: " + orderColumn);
        }

        rowIndexes.sort((a, b) -> {
            Object va = getCellValue(table, orderColumn, a);
            Object vb = getCellValue(table, orderColumn, b);

            // Les nulls sont placés en dernier
            if (va == null && vb == null) {
                return 0;
            }
            if (va == null) {
                return desc ? 1 : -1;
            }
            if (vb == null) {
                return desc ? -1 : 1;
            }

            int cmp = compare(va, vb);
            return desc ? -cmp : cmp;
        });
    }

    /**
     * Évalue si une ligne satisfait une condition WHERE.
     * Supporte les opérateurs : =, !=, >, >=, <, <=, LIKE.
     */
    private boolean matchesWhere(Table table, int rowIndex, String condition) {
        String trimmed = condition.trim();
        String[] operators = {">=", "<=", "!=", ">", "<", "="};

        for (String operator : operators) {
            int opIdx = trimmed.indexOf(operator);
            if (opIdx == -1) {
                continue;
            }

            String colName = trimmed.substring(0, opIdx).trim();
            // Supprime les guillemets simples autour des valeurs string
            String rawValue = trimmed.substring(opIdx + operator.length()).trim().replaceAll("^'|'$", "");

            if (!table.getData().containsKey(colName)) {
                throw new IllegalArgumentException("Unknown column in WHERE: " + colName);
            }

            Object cell = getCellValue(table, colName, rowIndex);
            if (cell == null) {
                return false;
            }

            // Comparaison numérique
            if (cell instanceof Number) {
                double left = ((Number) cell).doubleValue();
                double right;

                try {
                    right = Double.parseDouble(rawValue);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid numeric value in WHERE: " + rawValue);
                }

                return switch (operator) {
                    case "=" -> left == right;
                    case "!=" -> left != right;
                    case ">" -> left > right;
                    case ">=" -> left >= right;
                    case "<" -> left < right;
                    case "<=" -> left <= right;
                    default -> false;
                };
            }

            // Comparaison textuelle
            int cmp = cell.toString().compareTo(rawValue);
            return switch (operator) {
                case "=" -> cmp == 0;
                case "!=" -> cmp != 0;
                case ">" -> cmp > 0;
                case ">=" -> cmp >= 0;
                case "<" -> cmp < 0;
                case "<=" -> cmp <= 0;
                default -> false;
            };
        }

        // Support de l'opérateur LIKE (avec % et _)
        if (trimmed.toUpperCase().contains(" LIKE ")) {
            String[] parts = trimmed.split("(?i)\\sLIKE\\s");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid LIKE condition: " + condition);
            }

            String colName = parts[0].trim();
            // Conversion du pattern SQL en regex Java
            String pattern = parts[1].trim()
                    .replaceAll("^'|'$", "")
                    .replace("%", ".*")
                    .replace("_", ".");

            if (!table.getData().containsKey(colName)) {
                throw new IllegalArgumentException("Unknown column in LIKE: " + colName);
            }

            Object cell = getCellValue(table, colName, rowIndex);
            return cell != null && cell.toString().matches(pattern);
        }

        throw new IllegalArgumentException("Cannot parse WHERE condition: " + condition);
    }

    /**
     * Vérifie si une valeur satisfait un objet Filter (opérateur + valeur de référence).
     */
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

    /**
     * Construit la liste de lignes à partir d'index de lignes et de colonnes sélectionnées.
     */
    private List<List<Object>> buildRowsFromIndexes(Table table, List<Integer> rowIndexes, List<String> selectedColumns) {
        List<List<Object>> result = new ArrayList<>();

        for (int rowIndex : rowIndexes) {
            result.add(buildRow(table, rowIndex, selectedColumns));
        }

        return result;
    }

    /**
     * Construit une ligne à partir de son index et des colonnes demandées.
     */
    private List<Object> buildRow(Table table, int rowIndex, List<String> selectedColumns) {
        List<Object> row = new ArrayList<>();

        for (String columnName : selectedColumns) {
            row.add(getCellValue(table, columnName, rowIndex));
        }

        return row;
    }

    /**
     * Retourne la valeur d'une cellule à partir du nom de colonne et de l'index de ligne.
     */
    private Object getCellValue(Table table, String columnName, int rowIndex) {
        List<Object> columnData = table.getData().get(columnName);
        if (columnData == null) {
            throw new IllegalArgumentException("Unknown column: " + columnName);
        }
        if (rowIndex < 0 || rowIndex >= columnData.size()) {
            throw new IllegalArgumentException("Invalid row index: " + rowIndex);
        }
        return columnData.get(rowIndex);
    }

    /**
     * Récupère une table depuis le registre, ou lève une exception si introuvable.
     */
    private Table getTable(String tableName) {
        return registry.get(tableName)
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));
    }

    /**
     * Retourne la liste de toutes les valeurs d'une colonne.
     */
    private List<Object> getColumnValues(Table table, String column) {
        if (column == null || column.isBlank()) {
            throw new IllegalArgumentException("Column is required");
        }

        String trimmed = column.trim();
        if (!table.getData().containsKey(trimmed)) {
            throw new IllegalArgumentException("Unknown column: " + column);
        }

        return table.getData().get(trimmed);
    }

    /**
     * Convertit un objet en double. Lève une exception si la colonne n'est pas numérique.
     */
    private double toDouble(Object value, String column) {
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("Column is not numeric: " + column);
        }
        return ((Number) value).doubleValue();
    }

    /**
     * Compare deux objets Comparable. Utilisé pour MIN, MAX et ORDER BY.
     */
    @SuppressWarnings("unchecked")
    private int compare(Object a, Object b) {
        return ((Comparable<Object>) a).compareTo(b);
    }

    /**
     * Convertit une map de résultats d'agrégation en liste de lignes [clé, valeur].
     */
    private List<List<Object>> buildGroupAggregateResult(Map<Object, ?> aggregates) {
        List<List<Object>> result = new ArrayList<>();
        for (Map.Entry<Object, ?> entry : aggregates.entrySet()) {
            result.add(List.of(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    /**
     * Recherche insensible à la casse d'un mot-clé dans une chaîne.
     * Retourne l'index de début, ou -1 si absent.
     */
    private int upperIndexOf(String s, String keyword) {
        return s.toUpperCase().indexOf(keyword);
    }

    /**
     * Retourne la liste de tous les index de lignes d'une table.
     */
    private List<Integer> allRowIndexes(Table table) {
        List<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < table.rowCount(); i++) {
            indexes.add(i);
        }
        return indexes;
    }
}
