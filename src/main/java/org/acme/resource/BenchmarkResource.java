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

@Path("/api/benchmark")
@Produces(MediaType.APPLICATION_JSON)
public class BenchmarkResource {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkResource.class);

    @Inject TableRegistry registry;
    @Inject QueryExecutionService queryService;

    /**
     * Benchmark sur données synthétiques — pas besoin de fichier.
     * Génère N lignes en mémoire, mesure SELECT / WHERE / GROUP BY.
     *
     * GET /api/benchmark/synthetic?rows=1000000&repeat=3
     */
    @GET
    @Path("/synthetic")
    public Response syntheticBenchmark(
            @QueryParam("rows")   @DefaultValue("1000000") int rows,
            @QueryParam("repeat") @DefaultValue("3")       int repeat) {

        String tableName = "bench_" + System.nanoTime();

        try {
            // --- 1. Création de la table ---
            List<Column> columns = List.of(
                    col("id",              DataType.LONG),
                    col("passenger_count", DataType.INT),
                    col("trip_distance",   DataType.DOUBLE),
                    col("fare_amount",     DataType.DOUBLE),
                    col("payment_type",    DataType.STRING)
            );
            Table table = new Table(tableName, columns);
            registry.create(table);

            // --- 2. Insertion (mesure LOAD) ---
            String[] payments = {"Credit", "Cash", "Mobile"};
            Random rng = new Random(42);

            long insertStart = System.currentTimeMillis();
            List<List<Object>> batch = new ArrayList<>(10_000);
            for (int i = 0; i < rows; i++) {
                batch.add(List.of(
                        (long) i,
                        rng.nextInt(6) + 1,
                        Math.round((0.5 + rng.nextDouble() * 49.5) * 100.0) / 100.0,
                        Math.round((2.5 + rng.nextDouble() * 197.5) * 100.0) / 100.0,
                        payments[rng.nextInt(3)]
                ));
                // Insertion par batch de 10 000 pour ne pas surcharger la mémoire
                if (batch.size() == 10_000) {
                    registry.insertRows(tableName, batch);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) registry.insertRows(tableName, batch);
            long insertMs = System.currentTimeMillis() - insertStart;

            // --- 3. Benchmarks requêtes (moyenne sur repeat exécutions) ---
            long selectMs  = measureAvg(repeat, () ->
                    queryService.selectQuery(tableName, "SELECT *"));

            long whereMs   = measureAvg(repeat, () ->
                    queryService.selectQuery(tableName, "SELECT * WHERE fare_amount > 50"));

            long groupByMs = measureAvg(repeat, () ->
                    queryService.groupByCount(tableName, "payment_type"));

            long orderMs   = measureAvg(repeat, () ->
                    queryService.selectQuery(tableName, "SELECT * ORDER BY fare_amount LIMIT 100"));

            // --- 4. Résultat ---
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("rows",       rows);
            result.put("repeat",     repeat);
            result.put("insertMs",   insertMs);
            result.put("selectMs",   selectMs);
            result.put("whereMs",    whereMs);
            result.put("groupByMs",  groupByMs);
            result.put("orderLimitMs", orderMs);

            logger.info("Benchmark synthetic {} lignes → insert={}ms select={}ms where={}ms groupBy={}ms",
                    rows, insertMs, selectMs, whereMs, groupByMs);

            return Response.ok(result).build();

        } catch (Exception e) {
            logger.error("Erreur benchmark", e);
            return Response.serverError().entity(Map.of("error", e.getMessage())).build();
        } finally {
            // Nettoyage de la table temporaire
            try { registry.delete(tableName); } catch (Exception ignored) {}
        }
    }

    /**
     * Benchmark sur un vrai fichier Parquet.
     * Mesure le temps de LOAD puis les temps de requêtes.
     *
     * POST /api/benchmark/load?table=taxi
     */
    @POST
    @Path("/load")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response loadBenchmark(
            @RestForm("file") FileUpload fileUpload,
            @QueryParam("table")  @DefaultValue("bench_parquet") String tableName,
            @QueryParam("repeat") @DefaultValue("3") int repeat) {

        if (fileUpload == null) {
            return Response.status(400).entity(Map.of("error", "Fichier manquant")).build();
        }

        try {
            File file = fileUpload.uploadedFile().toFile();

            // Supprimer la table si elle existe déjà
            try { registry.delete(tableName); } catch (Exception ignored) {}

            // --- 1. LOAD ---
            long loadStart = System.currentTimeMillis();
            int inserted = ParquetImporter.loadParquet(file, tableName, registry);
            long loadMs = System.currentTimeMillis() - loadStart;

            // Récupère une colonne numérique et une colonne string pour les requêtes
            Table table = registry.get(tableName).orElseThrow();
            String numCol    = firstColOfType(table, DataType.DOUBLE, DataType.LONG, DataType.INT);
            String stringCol = firstColOfType(table, DataType.STRING);

            // --- 2. Requêtes ---
            long selectMs = measureAvg(repeat, () ->
                    queryService.selectQuery(tableName, "SELECT * LIMIT 1000"));

            long whereMs = -1;
            if (numCol != null) {
                // Calcule un seuil approximatif (25e percentile) pour le WHERE
                double threshold = estimateThreshold(table, numCol);
                final String whereQuery = "SELECT * WHERE " + numCol + " > " + threshold;
                whereMs = measureAvg(repeat, () ->
                        queryService.selectQuery(tableName, whereQuery));
            }

            long groupByMs = -1;
            if (stringCol != null) {
                final String col = stringCol;
                groupByMs = measureAvg(repeat, () ->
                        queryService.groupByCount(tableName, col));
            }

            // --- 3. Résultat ---
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("table",    tableName);
            result.put("rows",     inserted);
            result.put("loadMs",   loadMs);
            result.put("selectMs", selectMs);
            if (whereMs  >= 0) result.put("whereMs",   whereMs);
            if (groupByMs >= 0) result.put("groupByMs", groupByMs);
            if (numCol    != null) result.put("whereColumn",   numCol);
            if (stringCol != null) result.put("groupByColumn", stringCol);

            logger.info("Benchmark load {} : {} lignes load={}ms select={}ms where={}ms groupBy={}ms",
                    file.getName(), inserted, loadMs, selectMs, whereMs, groupByMs);

            return Response.ok(result).build();

        } catch (Exception e) {
            logger.error("Erreur benchmark load", e);
            return Response.serverError().entity(Map.of("error", e.getMessage())).build();
        }
    }

    /**
     * Lance une série de benchmarks synthétiques à différentes tailles.
     * Pratique pour générer les données du graphique d'un coup.
     *
     * GET /api/benchmark/series?repeat=3
     */
    @GET
    @Path("/series")
    public Response series(@QueryParam("repeat") @DefaultValue("3") int repeat) {
        int[] sizes = {100_000, 500_000, 1_000_000, 2_000_000, 4_000_000};
        List<Map<String, Object>> results = new ArrayList<>();

        for (int size : sizes) {
            try {
                Response r = syntheticBenchmark(size, repeat);
                if (r.getStatus() == 200) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> row = (Map<String, Object>) r.getEntity();
                    results.add(row);
                }
            } catch (Exception e) {
                logger.warn("Erreur pour taille {}: {}", size, e.getMessage());
            }
        }

        return Response.ok(results).build();
    }

    // --- Utilitaires ---

    private Column col(String name, DataType type) {
        Column c = new Column();
        c.setName(name);
        c.setType(type);
        return c;
    }

    private long measureAvg(int repeat, Runnable action) {
        long total = 0;
        for (int i = 0; i < repeat; i++) {
            long t = System.currentTimeMillis();
            action.run();
            total += System.currentTimeMillis() - t;
        }
        return total / repeat;
    }

    private String firstColOfType(Table table, DataType... types) {
        Set<DataType> set = new HashSet<>(Arrays.asList(types));
        for (Column c : table.getColumns()) {
            if (set.contains(c.getType())) return c.getName();
        }
        return null;
    }

    private double estimateThreshold(Table table, String colName) {
        Integer idx = table.getColIndex().get(colName);
        if (idx == null || table.getRows().isEmpty()) return 0;
        // Prend la valeur à ~25% des lignes comme seuil
        int sampleIdx = table.getRows().size() / 4;
        Object val = table.getRows().get(sampleIdx)[idx];
        if (val instanceof Number) return ((Number) val).doubleValue();
        return 0;
    }
}
