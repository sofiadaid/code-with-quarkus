# CLAUDE.md — Moteur de Données Haute Performance

## Contexte du projet

Projet universitaire (Licence Informatique) : moteur de base de données simplifié en **Java 17 + Quarkus 3.x**, accessible via une API REST.
Dataset : **NYC Yellow Taxi** (fichiers Parquet, 4M+ lignes).
Contrainte principale : tout le stockage, requêtage et optimisation doit être implémenté à la main (pas de BDD, pas d'ORM, pas de moteur de requêtes existant).

## Stack technique

- **Java 17**, **Quarkus 3.31.3**, **Maven**
- **Jackson** (JSON via Quarkus REST)
- **Apache Commons Lang3 + Collections4** (utilitaires)
- **Parquet-Hadoop 1.13.1** (lecture des fichiers Parquet uniquement)
- Tests : JUnit 5 via `quarkus-junit5`

## Architecture

```
src/main/java/org/acme/
├── model/
│   ├── Column.java                 — colonne : nom + DataType
│   ├── DataType.java               — enum : INT, LONG, DOUBLE, STRING, DATE
│   ├── Filter.java                 — clause WHERE : colonne, opérateur, valeur
│   └── Table.java                  — table : nom, colonnes, List<Object[]> rows, HashMap colIndex
├── service/
│   ├── TableRegistry.java          — registre ConcurrentHashMap + parser SQL complet
│   ├── QueryExecutionService.java  — SELECT, WHERE, GROUP BY COUNT
│   └── StorageService.java         — [VIDE] persistence disque à implémenter
├── resource/
│   ├── TableResource.java          — tous les endpoints REST
│   ├── BenchmarkResource.java      — [À CRÉER] mesure des performances
│   └── UiRessource.java            — UI web HTML
└── importer/
    └── ParquetImporter.java        — lit les fichiers Parquet, infère le schéma
```

## Endpoints API existants

| Méthode | Endpoint | Description |
|---------|----------|-------------|
| POST | `/api/tables` | Créer une table |
| GET | `/api/tables/{name}` | Schéma d'une table |
| POST | `/api/tables/{name}/rows` | Insérer des lignes |
| GET | `/api/tables/{name}/rows?offset=0&limit=100` | Lire les lignes (paginé) |
| POST | `/api/tables/{name}/import` | Importer un fichier Parquet (multipart) |
| GET | `/api/tables/{name}/select?columns=col1,col2` | SELECT colonnes |
| GET | `/api/tables/{name}/query?q=SELECT...` | SQL libre (SELECT/WHERE/ORDER BY/LIMIT) |
| POST | `/api/tables/{name}/select-where` | WHERE via JSON body |
| GET | `/api/tables/{name}/group-by?column=col` | GROUP BY COUNT |

## Conventions de code

- Les beans sont `@ApplicationScoped` (CDI Quarkus)
- `TableRegistry` est le point central — il contient le parser SQL et le registre des tables
- Les rows sont stockées comme `List<Object[]>` dans `Table.java`
- L'index colonne `colIndex` est un `HashMap<String, Integer>` (nom → index dans le tableau)
- `normalize()` dans TableRegistry fait un `.trim()` sur les noms de tables
- Pas de BDD, pas d'ORM — tout est in-memory

## Ce qui manque (priorité décroissante)

1. `BenchmarkResource.java` — mesure LOAD + SELECT sur N lignes (obligatoire pour la soutenance)
2. Index colonne dans `TableRegistry` — `HashMap<valeur → List<indices>>` pour WHERE O(1)
3. `StorageService.java` — sérialisation disque (évite de recharger 4M lignes au restart)
4. SUM / AVG / MIN / MAX dans le parser SQL de `TableRegistry.selectQuery()`
5. `DELETE /api/tables/{name}` — suppression de table
6. Opérateur `CONTAINS` dans `matchesWhere()` de `TableRegistry`
7. Fonctions date `YEAR()`, `MONTH()` dans les clauses WHERE

## Lancer le projet

```bash
# Dev mode (hot reload)
./mvnw quarkus:dev

# Tests
./mvnw test

# Build
./mvnw package
```

## Fichiers de référence

- `PLAN.md` — plan complet du projet avec ordre d'implémentation
- `STATUS.md` — état actuel de chaque fonctionnalité

## Contraintes importantes

- Ne pas utiliser PostgreSQL, MySQL, SQLite, H2, MongoDB
- Ne pas utiliser Hibernate, JPA, EclipseLink
- Ne pas utiliser Lucene, Elasticsearch
- `parquet-hadoop` est autorisé **uniquement pour lire le format Parquet** (pas pour stocker/requêter)
- Librairies autorisées : Jackson, Apache Commons, gestion des dates Java
