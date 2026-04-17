package org.acme.service;

import jakarta.enterprise.context.ApplicationScoped;
import org.acme.model.Column;
import org.acme.model.DataType;
import org.acme.model.Table;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registre central des tables en mémoire.
 * Gère la création, la récupération, la suppression et la manipulation des données des tables.
 * Thread-safe grâce à l'utilisation d'une ConcurrentHashMap.
 */
@ApplicationScoped
public class TableRegistry {

    /** Stockage des tables indexées par leur nom normalisé. */
    private final Map<String, Table> tables = new ConcurrentHashMap<>();

    /**
     * Crée et enregistre une nouvelle table.
     * Le nom est normalisé (trim) avant l'insertion.
     */
    public Table create(Table table) {

        if (table == null || StringUtils.isBlank(table.getName())) { // vérifie que la table et son nom ne sont pas vides
            throw new IllegalArgumentException("Table name is required");
        }

        // Normalisation du nom et initialisation de la table
        String name = normalize(table.getName()); // supprime les espaces superflus autour du nom
        table.setName(name); // applique le nom normalisé à l'objet table
        table.buildIndex(); // construit l'index
        table.initializeStorage(); // initialise les structures de stockage des données
        if (table.getColumns() == null) { // s'assure que la liste de colonnes n'est jamais null
            table.setColumns(new ArrayList<>()); // remplace null par une liste vide
        }
        // Insertion atomique : échoue si la table existe déjà
        Table previous = tables.putIfAbsent(name, table);
        if (previous != null) {
            throw new IllegalStateException("Table already exists: " + name);
        }

        return table;
    }

    /**
     * Recherche une table par son nom.
     */
    public Optional<Table> get(String name) {
        if (name == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(tables.get(normalize(name))); // normalise avant la recherche pour éviter les écarts d'espaces
    }

    /**
     * Supprime une table du registre.
     * Sans effet si le nom est null ou si la table n'existe pas.
     */
    public void delete(String name) {
        if (name == null) {
            return;
        }
        tables.remove(normalize(name)); // supprime la table correspondante, sans effet si elle n'existe pas
    }

    /**
     * Insère une ou plusieurs lignes dans une table existante.
     * Chaque ligne doit avoir exactement autant de valeurs que la table a de colonnes.
     * Les valeurs sont converties vers le type déclaré de chaque colonne.
     */
    public int insertRows(String tableName, List<List<Object>> inputRows) {
        Table table = get(tableName)
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName)); // la table doit exister avant d'insérer

        if (inputRows == null || inputRows.isEmpty()) { // on refuse un appel sans données
            throw new IllegalArgumentException("No rows provided"); // pas de ligne à insérer, inutile de continuer
        }

        int expected = table.getColumns().size(); // nombre de colonnes attendu pour chaque ligne
        int count = 0; // compteur des lignes insérées avec succès

        for (List<Object> row : inputRows) { // itération sur chaque ligne fournie
            // Vérification que la ligne a le bon nombre de valeurs
            if (row == null || row.size() != expected) { // taille incorrecte ou ligne nulle
                throw new IllegalArgumentException(
                        "Row size mismatch. Expected " + expected + " values, got " + (row == null ? 0 : row.size())
                ); // message précis pour faciliter le débogage
            }

            Object[] converted = new Object[expected]; // tableau de valeurs converties prêt à être stocké

            // Conversion de chaque valeur vers le type de la colonne correspondante
            for (int i = 0; i < expected; i++) { // parcourt chaque position dans la ligne
                Column column = table.getColumns().get(i); // récupère la définition de la colonne i
                converted[i] = convert(row.get(i), column.getType(), column.getName()); // convertit et valide la valeur
            }

            table.addRow(converted); // ajoute la ligne convertie dans le stockage de la table
            count++; // incrémente le compteur après une insertion réussie
        }

        return count; // retourne le nombre total de lignes insérées
    }

    /**
     * Retourne une page de lignes d'une table (pagination offset/limit).
     */
    public List<List<Object>> getRows(String tableName, int offset, int limit) {
        Table table = get(tableName)
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        // Valeurs par défaut pour les paramètres de pagination invalides
        if (offset < 0) {
            offset = 0;
        }
        if (limit <= 0) {
            limit = 100;
        }

        int rowCount = table.rowCount();
        // Calcul de l'index de fin en évitant de dépasser la taille réelle
        int end = Math.min(rowCount, offset + limit);

        List<List<Object>> result = new ArrayList<>();

        // Lecture colonne par colonne pour chaque ligne dans la plage demandée
        for (int rowIndex = offset; rowIndex < end; rowIndex++) {
            List<Object> row = new ArrayList<>();

            for (Column column : table.getColumns()) {
                row.add(table.getData().get(column.getName()).get(rowIndex));
            }

            result.add(row);
        }

        return result;
    }

    /**
     * Normalise un nom de table en supprimant les espaces en début et fin.
     */
    private String normalize(String s) {
        return s.trim();
    }

    /**
     * Convertit une valeur brute vers le type de données attendu par une colonne.
     * Supporte les types : INT, LONG, DOUBLE, STRING, DATE (ISO-8601).
     */
    private Object convert(Object value, DataType type, String colName) {
        if (value == null) {
            return null;
        }

        try {
            return switch (type) {
                case INT -> (value instanceof Number n) ? n.intValue() : Integer.parseInt(value.toString());
                case LONG -> (value instanceof Number n) ? n.longValue() : Long.parseLong(value.toString());
                case DOUBLE -> (value instanceof Number n) ? n.doubleValue() : Double.parseDouble(value.toString());
                case STRING -> value.toString();
                case DATE -> java.time.LocalDate.parse(value.toString());
            };
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid value for column '" + colName + "' (" + type + "): " + value
            );
        }
    }
}