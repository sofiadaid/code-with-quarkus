package org.acme.resource;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.acme.importer.ParquetImporter;
import org.acme.model.Column;
import org.acme.model.DataType;
import org.acme.model.Table;
import org.acme.service.QueryExecutionService;
import org.acme.service.TableRegistry;
import org.jboss.resteasy.reactive.RestForm;
import org.jboss.resteasy.reactive.multipart.FileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

// Endpoint principal de l'API
@Path("/api/benchmark")
@Produces(MediaType.APPLICATION_JSON)  // Toutes les réponses seront en JSON
public class BenchmarkResource {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkResource.class);

    // Injection du registre de tables (stockage en mémoire)
    @Inject
    TableRegistry registry;

    // Injection du moteur d'exécution de requêtes
    @Inject
    QueryExecutionService queryService;


    /**
     * ENDPOINT 1 : BENCHMARK SYNTHÉTIQUE
     *  Génère des données artificielles puis mesure les performances
     */
    @GET
    @Path("/synthetic")
    public Response syntheticBenchmark(
            @QueryParam("rows") @DefaultValue("1000000") int rows, // nombre de lignes à générer
            @QueryParam("repeat") @DefaultValue("3") int repeat) { // nombre de répétitions pour moyenne

        // Nom unique de table (évite les conflits)
        String tableName = "bench_" + System.nanoTime();

        // Mesure mémoire AVANT
        long beforeMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        try {
            List<Column> columns = List.of(
                    // Définition des colonnes de la table
                    col("id", DataType.LONG),
                    col("passenger_count", DataType.INT),
                    col("trip_distance", DataType.DOUBLE),
                    col("fare_amount", DataType.DOUBLE),
                    col("payment_type", DataType.STRING)
            );

            Table table = new Table(tableName, columns);
            registry.create(table);

            // Valeurs possibles pour paiement
            String[] payments = {"Credit", "Cash", "Mobile"};
            Random rng = new Random(42);

            // Début mesure insertion
            long insertStart = System.currentTimeMillis();
            // Batch pour insertion optimisée (évite insert ligne par ligne) crée un buffer
            List<List<Object>> batch = new ArrayList<>(10_000);

            // Génération des données
            for (int i = 0; i < rows; i++) {
                batch.add(List.of(
                        (long) i, // id
                        rng.nextInt(6) + 1, // passagers (1 à 6)
                        Math.round((0.5 + rng.nextDouble() * 49.5) * 100.0) / 100.0, // distance
                        Math.round((2.5 + rng.nextDouble() * 197.5) * 100.0) / 100.0, // prix
                        payments[rng.nextInt(3)] // type paiement
                ));

                // Quand batch plein → insertion but: 10 000 => 1 appel
                if (batch.size() == 10_000) {
                    registry.insertRows(tableName, batch);
                    batch.clear();
                }
            }

            // Insérer le reste
            if (!batch.isEmpty()) {
                registry.insertRows(tableName, batch);
            }

            // Temps total insertion
            long insertMs = System.currentTimeMillis() - insertStart;
            // Limite pour éviter requêtes trop lourdes
            int effectiveRows = Math.min(rows, 1000);

            //  Warmup JVM (important pour éviter des faux temps, active optimisation JVM)
            queryService.selectQuery(tableName, "SELECT * LIMIT  " + effectiveRows);
            queryService.selectQuery(tableName, "SELECT * WHERE fare_amount > 50 LIMIT  " + effectiveRows);

            // Mesure des requetes
            long selectMs = measureAvg(repeat, () ->
                    queryService.selectQuery(tableName, "SELECT * LIMIT  " + effectiveRows));

            long whereMs = measureAvg(repeat, () ->
                    queryService.selectQuery(tableName, "SELECT * WHERE fare_amount > 50 LIMIT  " + effectiveRows));

            long groupByMs = measureAvg(repeat, () ->
                    queryService.groupByCount(tableName, "payment_type"));

            long orderMs = measureAvg(repeat, () ->
                    queryService.selectQuery(tableName, "SELECT * ORDER BY fare_amount LIMIT 100"));


            // Résultat final (structure JSON)
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("nombreLignes", rows);
            result.put("tempsImportMs", insertMs);
            result.put("tempsSelectMs", selectMs);
            result.put("tempsWhereMs", whereMs);
            result.put("tempsGroupByMs", groupByMs);
            result.put("tempsOrderByMs", orderMs);


            logger.info(
                    "Benchmark {} lignes → import={}ms scan={}ms filtre={}ms groupBy={}ms tri={}ms",
                    rows, insertMs, selectMs, whereMs, groupByMs, orderMs
            );

            // Calcul mémoire utilisée
            long afterMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            long usedMem = (afterMem - beforeMem) / (1024 * 1024);

            result.put("memoireMB", usedMem);

            return Response.ok(result).build();

        } catch (Exception e) {
            logger.error("Erreur benchmark", e);
            return Response.serverError().entity(Map.of("error", e.getMessage())).build();
        } finally {
            // Nettoyage : suppression table
            try {
                registry.delete(tableName);
            } catch (Exception ignored) {
            }
        }
    }

    @POST
    @Path("/load") // URL : /api/benchmark/load
    @Consumes(MediaType.MULTIPART_FORM_DATA)  // On reçoit un fichier (form-data)
    public Response loadBenchmark(
            // Récupère le fichier envoyé (clé "file" côté client)
            @RestForm("file") FileUpload fileUpload,
            // Nom de la table (optionnel, valeur par défaut si absent)
            @QueryParam("table") @DefaultValue("bench_parquet") String tableName,
            // Nombre de répétitions pour calculer une moyenne
            @QueryParam("repeat") @DefaultValue("3") int repeat) {

        if (fileUpload == null) {
            return Response.status(400).entity(Map.of("error", "Fichier manquant")).build();
        }

        try {
            // Conversion du fichier uploadé en objet File Java
            File file = fileUpload.uploadedFile().toFile();

            // On supprime la table si elle existe déjà (évite conflits)
            try {
                registry.delete(tableName);
            } catch (Exception ignored) {
            }

            // Début du chronométrage du chargement
            long loadStart = System.currentTimeMillis();
            // Chargement du fichier parquet dans la table
            int inserted = ParquetImporter.loadParquet(file, tableName, registry);
            //  Temps total de chargement
            long loadMs = System.currentTimeMillis() - loadStart;

            // Récupération de la table
            Table table = registry.get(tableName).orElseThrow();

            // On cherche automatiquement une colonne numérique
            String numCol = firstColOfType(table, DataType.DOUBLE, DataType.LONG, DataType.INT);

            // On cherche une colonne texte
            String stringCol = firstColOfType(table, DataType.STRING);


            //bench Select
            long selectMs = measureAvg(repeat, () ->
                    queryService.selectQuery(tableName, "SELECT * LIMIT 1000"));


            long whereMs = -1;
            if (numCol != null) {
                // On calcule une valeur seuil réaliste
                double threshold = estimateThreshold(table, numCol);
                // Construction dynamique de la requête WHERE
                final String whereQuery = "SELECT * WHERE " + numCol + " > " + threshold;
                // Mesure du temps d'exécution
                whereMs = measureAvg(repeat, () ->
                        queryService.selectQuery(tableName, whereQuery));
            }

            long groupByMs = -1;
            if (stringCol != null) {
                final String col = stringCol;
                groupByMs = measureAvg(repeat, () ->
                        queryService.groupByCount(tableName, col));
            }

            // Construction du résultat JSON
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("table", tableName); // nom de la table
            result.put("rows", inserted);
            result.put("loadMs", loadMs);
            result.put("selectMs", selectMs);

            if (whereMs >= 0) {
                result.put("whereMs", whereMs);
            }
            if (groupByMs >= 0) {
                result.put("groupByMs", groupByMs);
            }
            if (numCol != null) {
                result.put("whereColumn", numCol);
            }
            if (stringCol != null) {
                result.put("groupByColumn", stringCol);
            }

            logger.info(
                    "Benchmark load {} : {} lignes load={}ms select={}ms where={}ms groupBy={}ms",
                    file.getName(), inserted, loadMs, selectMs, whereMs, groupByMs
            );

            // Retour HTTP 200 avec résultats
            return Response.ok(result).build();

        } catch (Exception e) {
            logger.error("Erreur benchmark load", e);
            return Response.serverError().entity(Map.of("error", e.getMessage())).build();
        }
    }

    @GET
    @Path("/series") // URL : /api/benchmark/series
    public Response series(@QueryParam("repeat") @DefaultValue("3") int repeat) {
        // Tailles de dataset à tester
        int[] sizes = {100, 500, 1000};
        // Liste des résultats
        List<Map<String, Object>> results = new ArrayList<>();

        for (int size : sizes) {
            try {
                // On appelle le benchmark synthétique
                Response response = syntheticBenchmark(size, repeat);

                if (response.getStatus() == 200) {
                    @SuppressWarnings("unchecked")
                    // Conversion du résultat en Map
                    Map<String, Object> row = (Map<String, Object>) response.getEntity();
                    // Ajout dans la liste
                    results.add(row);
                }
            } catch (Exception e) {
                logger.warn("Erreur pour taille {}: {}", size, e.getMessage());
            }
        }

        // Retourne tous les résultats (tableau JSON)
        return Response.ok(results).build();
    }

    // Crée une colonne
    private Column col(String name, DataType type) {
        Column c = new Column();
        c.setName(name);
        c.setType(type);
        return c;
    }

    // Mesure le temps moyen d'une opération
    private long measureAvg(int repeat, Runnable action) {
        long total = 0;

        for (int i = 0; i < repeat; i++) {
            long start = System.currentTimeMillis();
            action.run();
            total += System.currentTimeMillis() - start;
        }

        return total / repeat;
    }

    // Trouve la première colonne d’un certain type
    private String firstColOfType(Table table, DataType... types) {
        Set<DataType> set = new HashSet<>(Arrays.asList(types));

        for (Column c : table.getColumns()) {
            if (set.contains(c.getType())) {
                return c.getName();
            }
        }

        return null;
    }

    // choisir automatiquement un bon seuil pour tester un filtre WHERE
    private double estimateThreshold(Table table, String colName) {
        List<Object> columnData = table.getData().get(colName);

        if (columnData == null || columnData.isEmpty()) {
            return 0;
        }

        // prend une valeur au milieu (approximation)
        int sampleIdx = columnData.size() / 4;
        Object value = columnData.get(sampleIdx);

        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }

        return 0;
    }
}