# Moteur de Données Haute Performance — Plan du projet

## Vue d'ensemble

Projet Licence Informatique — moteur de base de données simplifié en Java/Quarkus.
Dataset utilisé : NYC Yellow Taxi (fichiers Parquet).
Objectif : ingestion, stockage, requêtage SQL-like, optimisation des performances sur 4M+ lignes.

---

## Ce qui est déjà implémenté

| Composant | Fichier | État |
|-----------|---------|------|
| API REST (CREATE, INSERT, SELECT) | `TableResource.java` | ✅ Complet |
| Modèle Table / Column / DataType | `model/` | ✅ Complet |
| Import Parquet | `ParquetImporter.java` | ✅ Complet |
| SELECT * et colonnes spécifiques | `TableRegistry.java` | ✅ Complet |
| WHERE (=, >, <, >=, <=, !=, LIKE) | `QueryExecutionService.java` | ✅ Complet |
| GROUP BY + COUNT | `QueryExecutionService.java` | ✅ Basique |
| ORDER BY + LIMIT | `TableRegistry.java` | ✅ Complet |
| Web UI | `UiRessource.java` | ✅ Complet |
| StorageService (persistence disque) | `StorageService.java` | ❌ Vide |
| Benchmarks | — | ❌ Absent |
| Optimisation avec index colonne | — | ❌ Absent |
| SUM / AVG / MIN / MAX | — | ❌ Absent |

---

## Ce qu'il reste à faire

### OBLIGATOIRE — Impact direct sur la note

#### 1. Benchmarks (4 pts soutenance + pts perf)

Le prof exige des mesures sur **4 millions de lignes minimum** avec le format :
> Nombre de lignes / Temps d'exécution

**Ce qu'il faut mesurer :**
- Temps de LOAD du fichier Parquet
- Temps d'exécution SELECT *
- Temps d'exécution WHERE (ex: `passenger_count = 2`)
- Temps d'exécution GROUP BY

**Format tableau attendu :**

| Nb lignes | LOAD (ms) | SELECT (ms) | WHERE (ms) | GROUP BY (ms) |
|-----------|-----------|-------------|------------|---------------|
| 100 000   | ?         | ?           | ?          | ?             |
| 500 000   | ?         | ?           | ?          | ?             |
| 1 000 000 | ?         | ?           | ?          | ?             |
| 4 000 000 | ?         | ?           | ?          | ?             |

Ce tableau doit exister en **deux versions** : avant optimisation et après optimisation.
Les résultats doivent être mis en **graphiques** (Python, Excel, Google Sheets...).

**Fichier à créer :** `BenchmarkResource.java`
- Endpoint : `GET /api/benchmark?rows=1000000`
- Génère ou charge N lignes depuis le fichier Yellow Taxi
- Retourne les temps mesurés en JSON

---

#### 2. Optimisation des performances (cœur du 12 pts)

**Problème actuel :**
Chaque requête WHERE parcourt toutes les lignes une par une → O(n).
Sur 4M lignes c'est lent.

**Solution : Index par colonne (HashMap)**
```
HashMap<String, HashMap<Object, List<Integer>>>
  colonne      valeur         indices des lignes
```

Exemple :
```
index["passenger_count"][2] = [0, 5, 12, 47, ...]  ← indices des lignes où passenger_count = 2
```

Requête `WHERE passenger_count = 2` → O(1) au lieu de O(n).

**Où implémenter :** `TableRegistry.java`
- Méthode `buildColumnIndex(String colName)` — construit l'index à la demande
- Méthode `getRowsByIndex(String col, Object val)` — lookup O(1)
- Modifier `selectQuery()` pour utiliser l'index quand disponible

**Mesure attendue :**
Relancer les benchmarks après → montrer le gain (ex: WHERE passe de 800ms à 5ms sur 4M lignes).

---

#### 3. StorageService — Persistence disque

**Problème actuel :** `StorageService.java` est vide. Si Quarkus redémarre, les 4M lignes sont perdues et il faut tout recharger.

**Solution : Sérialisation JSON ou binaire sur disque**

Deux options :
- **Option A (simple)** : sérialiser chaque table en JSON sur disque → lent mais simple
- **Option B (recommandée)** : format binaire custom (écrire les rows en binaire) → rapide

**Fichier :** `StorageService.java`
- `save(Table table)` → écrit sur disque dans `./data/{tableName}.bin`
- `load(String tableName)` → relit depuis disque au démarrage
- Appeler `save()` après chaque `insertRows()` dans `TableRegistry`

---

### IMPORTANT POUR LA NOTE — Bonus faciles

#### 4. Fonctions d'agrégation SUM / AVG / MIN / MAX

Ajouter dans le SQL parser (`selectQuery()` dans `TableRegistry.java`) :
```sql
SELECT SUM(fare_amount) FROM yellow_taxi WHERE passenger_count = 2
SELECT AVG(trip_distance) FROM yellow_taxi GROUP BY passenger_count
SELECT MIN(fare_amount), MAX(fare_amount) FROM yellow_taxi
```

Endpoint déjà disponible : `GET /api/tables/{name}/query?sql=...`
Il suffit d'étendre le parser pour reconnaître `SUM(col)`, `AVG(col)`, `MIN(col)`, `MAX(col)`.

---

#### 5. Suppression de table

**Fichiers :** `TableRegistry.java` + `TableResource.java`

```java
// TableRegistry.java
public void delete(String name) {
    tables.remove(name);
}

// TableResource.java
@DELETE
@Path("/api/tables/{name}")
public Response deleteTable(@PathParam("name") String name) {
    registry.delete(name);
    return Response.noContent().build();
}
```

---

#### 6. Opérateur CONTAINS dans WHERE

**Fichier :** `QueryExecutionService.java` (méthode `match()`)

```sql
WHERE store_and_fwd_flag CONTAINS 'Y'
```

Ajouter le cas `"CONTAINS"` dans le switch des opérateurs → `String.contains()`.

---

### BONUS AVANCÉ

#### 7. Fonctions sur les dates (YEAR, MONTH)

```sql
WHERE YEAR(tpep_pickup_datetime) = 2024
WHERE MONTH(tpep_pickup_datetime) = 3
```

Modifier le parser dans `TableRegistry.java` pour détecter `YEAR(col)` et `MONTH(col)` dans les conditions WHERE.

---

## Ordre d'implémentation recommandé

```
Étape 1 — Benchmarks (sans optimisation)
  → Charger le fichier Yellow Taxi complet
  → Mesurer LOAD + SELECT + WHERE + GROUP BY sur 100K / 500K / 1M / 4M lignes
  → Enregistrer les résultats comme "baseline avant optimisation"

Étape 2 — Index colonne
  → Implémenter HashMap d'index dans TableRegistry
  → Relancer les benchmarks → comparer avec étape 1

Étape 3 — SUM / AVG / MIN / MAX
  → Étendre le parser SQL
  → Tester sur le dataset Yellow Taxi

Étape 4 — StorageService
  → Sérialisation disque basique
  → Vérifier que le reload est plus rapide que le re-import Parquet

Étape 5 — Suppression de table + CONTAINS
  → Simple, rapide à faire

Étape 6 — Graphiques benchmarks
  → Exporter les résultats JSON en CSV
  → Tracer les courbes (Python matplotlib ou Excel)
```

---

## Architecture des fichiers

```
src/main/java/org/acme/
├── model/
│   ├── Column.java                  ← Définition d'une colonne (nom + type)
│   ├── DataType.java                ← Enum : INT, LONG, DOUBLE, STRING, DATE
│   ├── Filter.java                  ← Clause WHERE (colonne, opérateur, valeur)
│   └── Table.java                   ← Structure table (nom, colonnes, lignes, index)
├── service/
│   ├── TableRegistry.java           ← Registre des tables + parser SQL complet
│   ├── QueryExecutionService.java   ← SELECT, WHERE, GROUP BY, agrégations
│   └── StorageService.java          ← [À IMPLÉMENTER] Persistence disque
├── resource/
│   ├── TableResource.java           ← Endpoints API REST
│   ├── BenchmarkResource.java       ← [À CRÉER] Endpoints benchmarks
│   └── UiRessource.java             ← UI web
└── importer/
    └── ParquetImporter.java         ← Import fichiers Parquet
```

---

## Contraintes du prof à respecter

| Contrainte | Notre situation |
|-----------|----------------|
| ❌ Pas de BDD existante (PostgreSQL, SQLite...) | ✅ OK — tout fait à la main |
| ❌ Pas d'ORM (Hibernate, JPA...) | ✅ OK |
| ❌ Pas de moteur de requêtes (Lucene...) | ✅ OK |
| ✅ Librairies autorisées : JSON, dates, Apache Commons | ✅ OK |
| ⚠️ Parquet-Hadoop (lib externe) | Justifier à la soutenance |

**Argument pour Parquet-Hadoop à la soutenance :**
> "On utilise Parquet-Hadoop uniquement pour lire le format binaire columnar du fichier Parquet.
> Tout le stockage en mémoire, le requêtage et l'optimisation sont implémentés à la main.
> C'est l'équivalent d'utiliser un parser JSON — on lit le format, on ne délègue pas la logique métier."

---

## Notation — Comment maximiser les points

### 12 pts — Fonctionnalités + performances
- SELECT / WHERE / GROUP BY fonctionnels sur le dataset du prof → base
- Benchmarks avec courbes avant/après → perf
- SUM, AVG, MIN, MAX → bonus
- CONTAINS, fonctions date → bonus avancé

### 4 pts — Qualité du code
- Architecture en couches (model / service / resource) → déjà bien fait
- Tests qui passent → supprimer ou corriger `CsvImporterTest.java`
- Code lisible, sans duplication

### 4 pts — Soutenance
- Montrer les graphiques benchmark et les commenter
- Expliquer le choix in-memory + index HashMap
- Justifier Parquet-Hadoop
- Démontrer les requêtes en live sur le dataset Yellow Taxi

---

## Dataset — NYC Yellow Taxi

Téléchargement : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Colonnes utiles pour les benchmarks et démos :
- `passenger_count` — INT (1-6)
- `trip_distance` — DOUBLE
- `fare_amount` — DOUBLE
- `total_amount` — DOUBLE
- `tpep_pickup_datetime` — DATE/STRING
- `payment_type` — INT

Requêtes de démo typiques :
```sql
SELECT * FROM yellow_taxi WHERE passenger_count = 2 LIMIT 100
SELECT AVG(fare_amount) FROM yellow_taxi GROUP BY passenger_count
SELECT COUNT(*) FROM yellow_taxi WHERE trip_distance > 10
SELECT MIN(fare_amount), MAX(fare_amount) FROM yellow_taxi
```
