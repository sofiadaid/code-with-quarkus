# STATUS.md — État du projet

Dernière mise à jour : 2026-04-11 (session 3)
Branche active : `feature/query-execution`

---

## Résumé de la session de travail

### Refactoring qualité du code (session 3)

#### 1. `selectQuery()` déplacé de `TableRegistry` vers `QueryExecutionService`
**Problème :** `TableRegistry` faisait trop de choses — il gérait le stockage ET parsait le SQL.

**Correction :**
- `selectQuery()`, `matchesWhere()`, `upperIndexOf()` déplacés dans `QueryExecutionService`
- `TableRegistry` ne fait plus que stocker/récupérer les tables et les lignes
- `TableResource` et `BenchmarkResource` appellent maintenant `queryExecutionService.selectQuery()`

#### 2. Suppression de la variable inutilisée `selectedNames`
**Problème :** `List<String> selectedNames` était construite dans `selectQuery()` mais jamais utilisée.

**Correction :** Variable supprimée.

#### 3. Formatage corrigé dans `TableRegistry`
**Problème :** Espaces manquants autour des opérateurs, accolades mal placées.

**Correction :** Formatage uniformisé sur tout le fichier.

#### 4. `StorageService` documenté
**Problème :** Classe vide sans explication.

**Correction :** Ajout d'un Javadoc avec les TODO expliqués (`save()`, `load()`).

---

### Bugs corrigés

#### 1. `ParquetImporter.java` — Lecture des colonnes FLOAT
**Problème :** Les colonnes de type `FLOAT` dans un fichier Parquet étaient mappées en `DataType.DOUBLE`
mais lues avec `group.getDouble()` → crash à l'import si le fichier contient des FLOAT.

**Correction :** Lecture avec `group.getFloat()` puis conversion en `double` :
```java
if (primitiveType == FLOAT) return (double) group.getFloat(colIndex, 0);
return group.getDouble(colIndex, 0);
```

---

#### 2. `ParquetImporter.java` — Lecture des colonnes BINARY/STRING
**Problème :** Les colonnes `BINARY` sans annotation UTF8 tombaient dans le `default`
du switch avec `getValueToString()` de façon incohérente.

**Correction :** Mapping explicite `BINARY` et `FIXED_LEN_BYTE_ARRAY` → `DataType.STRING`,
lecture unifiée via `getValueToString()` pour tous les types STRING.

---

#### 3. `UiRessource.java` — Noms de colonnes dans SELECT *
**Problème :** Quand on exécutait `SELECT *` dans la section "Requête SELECT" de l'UI,
les colonnes s'affichaient comme `col0, col1, col2...` au lieu des vrais noms.

**Correction :** `doQuery()` fetche maintenant le schéma de la table avant d'afficher
les résultats → les vrais noms de colonnes sont toujours affichés.

---

#### 4. Tests — `CsvImporterTest.java` et `GreetingResourceIT.java` supprimés
**Problème :** `CsvImporterTest.java` référençait `CsvImporter` et `Columntable`
qui avaient été supprimés → compilation échouait.
`GreetingResourceIT.java` étendait `GreetingResourceTest` qui n'existait plus.

**Correction :** Les deux fichiers supprimés (pas de CSV dans le projet, endpoint `/hello` jamais créé).

---

#### 5. `GreetingResourceTest.java` supprimé
**Problème :** Test template Quarkus qui testait un endpoint `/hello` inexistant → échec systématique.

**Correction :** Fichier supprimé.

---

#### 6. `QueryExecutionServiceTest.java` — `$.size()` incorrect
**Problème :** RestAssured utilisait `"$.size()"` (syntaxe JsonPath incorrecte)
au lieu de `"size()"` → les assertions retournaient `null` même quand les données étaient correctes.

**Correction :** Remplacé `"$.size()"` par `"size()"` dans toutes les assertions.

---

#### 7. Tests — Conflit de nom de table `users`
**Problème :** `QueryExecutionServiceTest` et `TableRessourceTest` créaient tous les deux
une table nommée `users` → le deuxième test recevait un 400 "Table already exists".

**Correction :**
- `QueryExecutionServiceTest` utilise la table `test_query` avec `@Order` pour garantir l'ordre d'exécution
- `TableRessourceTest` utilise `users_resource_` + timestamp pour chaque test

---

### Fichiers ajoutés

| Fichier | Description |
|---------|-------------|
| `PLAN.md` | Plan complet du projet avec ordre d'implémentation |
| `CLAUDE.md` | Contexte projet pour Claude (architecture, conventions, contraintes) |
| `STATUS.md` | Ce fichier — état du projet et suivi des tâches |
| `BenchmarkResource.java` | Nouveaux endpoints de mesure des performances |

---

### Tests ajoutés / réécrits

| Fichier | Ce qui a changé |
|---------|----------------|
| `QueryExecutionServiceTest.java` | Réécrit : `@Order`, table unique, fix `$.size()`, ajout GROUP BY et SQL parser |
| `TableRessourceTest.java` | Réécrit : noms uniques, ajout test duplicate et 404 |
| `ParquetImportIntegrationTest.java` | Nouveau : test end-to-end import Parquet + SELECT/WHERE/GROUP BY |

---

### Dépendance ajoutée

`hadoop-mapreduce-client-core` en scope `test` dans `pom.xml` — nécessaire uniquement
pour écrire des fichiers Parquet dans les tests d'intégration (pas utilisé en production).

---

## État actuel des fonctionnalités

### Infrastructure & API

| Fonctionnalité | Fichier | État |
|----------------|---------|------|
| Démarrage Quarkus | `pom.xml` | ✅ OK |
| API REST de base | `TableResource.java` | ✅ OK |
| Web UI | `UiRessource.java` | ✅ OK (fix SELECT *) |
| OpenAPI / Swagger | `pom.xml` | ✅ OK |

---

### Modèle de données

| Fonctionnalité | Fichier | État |
|----------------|---------|------|
| Table (nom + colonnes + rows) | `model/Table.java` | ✅ OK |
| Colonne (nom + type) | `model/Column.java` | ✅ OK |
| Types : INT, LONG, DOUBLE, STRING, DATE | `model/DataType.java` | ✅ OK |
| Filtre WHERE | `model/Filter.java` | ✅ OK |

---

### Import de données

| Fonctionnalité | Fichier | État |
|----------------|---------|------|
| Import Parquet (INT32, INT64, DOUBLE) | `ParquetImporter.java` | ✅ OK |
| Import Parquet colonnes FLOAT | `ParquetImporter.java` | ✅ Corrigé |
| Import Parquet colonnes STRING/BINARY | `ParquetImporter.java` | ✅ Corrigé |
| Valeurs nulles (colonnes optionnelles) | `ParquetImporter.java` | ✅ OK |
| Import CSV | — | — Non prévu (uniquement Parquet) |

---

### Requêtes SQL

| Fonctionnalité | Fichier | État |
|----------------|---------|------|
| SELECT * | `TableRegistry.java` | ✅ OK |
| SELECT colonnes spécifiques | `TableRegistry.java` | ✅ OK |
| WHERE = | `TableRegistry.java` | ✅ OK |
| WHERE >, <, >=, <=, != | `TableRegistry.java` | ✅ OK |
| WHERE LIKE (% et _) | `TableRegistry.java` | ✅ OK |
| ORDER BY ASC / DESC | `TableRegistry.java` | ✅ OK |
| LIMIT | `TableRegistry.java` | ✅ OK |
| GROUP BY + COUNT | `QueryExecutionService.java` | ✅ OK |
| WHERE CONTAINS | — | ❌ Pas encore implémenté |
| GROUP BY + SUM / AVG / MIN / MAX | — | ❌ Pas encore implémenté |
| Fonctions date YEAR(), MONTH() | — | ❌ Pas encore implémenté |

---

### Stockage

| Fonctionnalité | Fichier | État |
|----------------|---------|------|
| Stockage in-memory thread-safe | `TableRegistry.java` | ✅ OK |
| Persistence disque (save/load) | `StorageService.java` | ❌ Vide |

---

### Benchmarks et performances

| Fonctionnalité | Fichier | État |
|----------------|---------|------|
| Scan linéaire WHERE (actuel) | `TableRegistry.java` | ✅ En place — O(n) |
| `GET /api/benchmark/synthetic?rows=N` | `BenchmarkResource.java` | ✅ Implémenté |
| `GET /api/benchmark/series` | `BenchmarkResource.java` | ✅ Implémenté |
| `POST /api/benchmark/load` (fichier Parquet) | `BenchmarkResource.java` | ✅ Implémenté |
| Index colonne HashMap pour WHERE = | `TableRegistry.java` | ❌ À faire |
| Graphiques avant/après optimisation | — | ❌ À faire (Python/Excel) |

---

### Tests

| Fichier | Tests | État |
|---------|-------|------|
| `ParquetImportIntegrationTest.java` | 7 tests (import + SELECT + WHERE + GROUP BY sur Parquet réel) | ✅ |
| `QueryExecutionServiceTest.java` | 6 tests (create, WHERE =, WHERE >, WHERE invalide, GROUP BY, SQL parser) | ✅ |
| `TableRessourceTest.java` | 3 tests (create, duplicate, 404) | ✅ |
| `TableRowsTest.java` | 1 test (insert + get rows) | ✅ |
| **Total** | **17 tests** | ✅ BUILD SUCCESS |

---

## Comment tester les benchmarks

### Prérequis
Lancer le serveur :
```bash
./mvnw quarkus:dev
```
Attendre : `Listening on: http://localhost:8080`

---

### Étape 1 — Benchmark synthétique (sans fichier)

Ouvre ces URLs dans ton navigateur une par une et **note les résultats** :

```
http://localhost:8080/api/benchmark/synthetic?rows=100000
http://localhost:8080/api/benchmark/synthetic?rows=500000
http://localhost:8080/api/benchmark/synthetic?rows=1000000
http://localhost:8080/api/benchmark/synthetic?rows=2000000
http://localhost:8080/api/benchmark/synthetic?rows=4000000
```

Ou lance la série complète d'un coup (prend ~5 minutes) :
```
http://localhost:8080/api/benchmark/series
```

**Exemple de résultat JSON :**
```json
{
  "rows": 1000000,
  "repeat": 3,
  "insertMs": 420,
  "selectMs": 210,
  "whereMs": 185,
  "groupByMs": 95,
  "orderLimitMs": 380
}
```

**Ce que mesure chaque champ :**
| Champ | Requête exécutée |
|-------|-----------------|
| `insertMs` | Temps d'insertion de N lignes en mémoire (= LOAD) |
| `selectMs` | `SELECT *` sur toutes les lignes |
| `whereMs` | `SELECT * WHERE fare_amount > 50` |
| `groupByMs` | `GROUP BY payment_type` (COUNT) |
| `orderLimitMs` | `SELECT * ORDER BY fare_amount LIMIT 100` |

---

### Étape 2 — Benchmark sur le vrai fichier Yellow Taxi (LOAD réel)

Dans un terminal :
```bash
curl -F "file=@/chemin/vers/yellow_tripdata_2024-01.parquet" \
     "http://localhost:8080/api/benchmark/load?table=taxi_bench"
```

Remplace `/chemin/vers/` par le vrai chemin du fichier.

**Résultat attendu :**
```json
{
  "table": "taxi_bench",
  "rows": 2964624,
  "loadMs": 18500,
  "selectMs": 850,
  "whereMs": 420,
  "groupByMs": 180,
  "whereColumn": "fare_amount",
  "groupByColumn": "store_and_fwd_flag"
}
```

---

### Étape 3 — Construire le tableau pour la soutenance

Note les résultats dans ce tableau (à remplir) :

| Nb lignes | insertMs | selectMs | whereMs | groupByMs |
|-----------|----------|----------|---------|-----------|
| 100 000   | ?        | ?        | ?       | ?         |
| 500 000   | ?        | ?        | ?       | ?         |
| 1 000 000 | ?        | ?        | ?       | ?         |
| 2 000 000 | ?        | ?        | ?       | ?         |
| 4 000 000 | ?        | ?        | ?       | ?         |

Ce tableau sera la **baseline "avant optimisation"**.
Après l'ajout de l'index colonne, on relancera les mêmes benchmarks pour montrer le gain.

---

### Étape 4 — Faire les graphiques (Python)

Copie les résultats dans un fichier `benchmark_avant.csv` :
```csv
rows,insertMs,selectMs,whereMs,groupByMs
100000,X,X,X,X
500000,X,X,X,X
1000000,X,X,X,X
2000000,X,X,X,X
4000000,X,X,X,X
```

Script Python pour tracer les courbes :
```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('benchmark_avant.csv')

plt.figure(figsize=(10, 6))
plt.plot(df['rows'], df['selectMs'],  marker='o', label='SELECT *')
plt.plot(df['rows'], df['whereMs'],   marker='s', label='WHERE')
plt.plot(df['rows'], df['groupByMs'], marker='^', label='GROUP BY')
plt.xlabel('Nombre de lignes')
plt.ylabel('Temps (ms)')
plt.title('Performance avant optimisation')
plt.legend()
plt.grid(True)
plt.savefig('benchmark_avant.png')
plt.show()
```

---

## Ce qui reste à faire

### Obligatoire pour la note

```
[x] 1. BenchmarkResource.java — FAIT
        → GET /api/benchmark/synthetic?rows=N
        → GET /api/benchmark/series
        → POST /api/benchmark/load

[ ] 2. Remplir le tableau benchmark (avant optimisation)
        → lancer /api/benchmark/series et noter les résultats

[ ] 3. Index colonne (optimisation WHERE)
        → HashMap<valeur → List<indices>> dans TableRegistry
        → WHERE = passe de O(n) à O(1)
        → relancer les benchmarks après pour comparer

[ ] 4. Graphiques avant/après
        → CSV + courbes Python ou Excel
        → obligatoire pour la soutenance
```

### Bonus (maximise les 12 pts)

```
[ ] 4. SUM / AVG / MIN / MAX
        → étendre le parser SQL dans TableRegistry.selectQuery()
        → ex: SELECT AVG(fare_amount) FROM taxi GROUP BY passenger_count

[ ] 5. Suppression de table
        → TableRegistry.delete(name)
        → DELETE /api/tables/{name}

[ ] 6. CONTAINS dans WHERE
        → ajouter l'opérateur dans matchesWhere() de TableRegistry
        → ex: WHERE store_and_fwd_flag CONTAINS 'Y'

[ ] 7. StorageService (persistence disque)
        → sérialisation binaire dans ./data/{tableName}.bin
        → évite de recharger 4M lignes à chaque redémarrage
```

### Bonus avancé

```
[ ] 8. Fonctions date YEAR() / MONTH()
        → détecter YEAR(col) et MONTH(col) dans les clauses WHERE
        → ex: WHERE YEAR(tpep_pickup_datetime) = 2024
```

---

## Score estimé

| Critère | Points dispo | Estimé aujourd'hui | Avec benchmarks + bonus |
|---------|-------------|-------------------|------------------------|
| Fonctionnalités de base | 6/12 | ✅ En place | ✅ |
| Performances + benchmarks | 4/12 | ❌ Manquant | ✅ si fait |
| Bonus (SUM, AVG, DELETE...) | 2/12 | ❌ Manquant | ✅ si fait |
| Qualité du code | 4/4 | ✅ Refactoré + 17 tests | ✅ |
| Soutenance + graphiques | 4/4 | ❌ Benchmarks manquants | ✅ si fait |
| **Total** | **20** | **~10/20** | **~18-20/20** |
