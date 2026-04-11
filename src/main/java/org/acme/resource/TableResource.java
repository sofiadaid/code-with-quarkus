package org.acme.resource;


import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.acme.model.Table;
import org.acme.service.TableRegistry;
import org.acme.service.QueryExecutionService;
import org.acme.model.Filter;
import org.jboss.resteasy.reactive.multipart.FileUpload;
import org.jboss.resteasy.reactive.RestForm;
import org.acme.importer.ParquetImporter;


@Path("/api/tables")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableResource {

    private static final Logger logger = LoggerFactory.getLogger(TableResource.class);

    @Inject
    TableRegistry registry;

    @Inject
    QueryExecutionService queryExecutionService;

    @POST
    public Response create(Table req) {
        try {
            Table created = registry.create(req);
            return Response.status(Response.Status.CREATED).entity(created).build();
        } catch (Exception e) {
            logger.error("Erreur création table", e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }

    @GET
    @Path("/{name}")
    public Response get(@PathParam("name") String name) {
        return registry.get(name)
                .map(t -> Response.ok(t).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity(Map.of("error", "Table not found: " + name))
                        .build());
    }


    @POST
    @Path("/{name}/rows")
    public Response insertRows(@PathParam("name") String name, java.util.List<java.util.List<Object>> inputRows) {
        try {
            int inserted = registry.insertRows(name, inputRows);
            return Response.status(Response.Status.CREATED)
                    .entity(java.util.Map.of("inserted", inserted))
                    .build();
        } catch (Exception e) {
            logger.error("Erreur lors de l'insertion dans la table", e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(java.util.Map.of("error", e.getMessage()))
                    .build();
        }
    }

    @GET
    @Path("/{name}/rows")
    public Response listRows(@PathParam("name") String name,
                             @QueryParam("offset") @DefaultValue("0") int offset,
                             @QueryParam("limit") @DefaultValue("100") int limit) {
        try {
            return Response.ok(registry.getRows(name, offset, limit)).build();
        } catch (Exception e) {
            logger.error("Erreur lors de la récupération des lignes de la table", e);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(java.util.Map.of("error", e.getMessage()))
                    .build();
        }
    }


    @POST
    @Path("/{name}/import")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response importParquet(
            @PathParam("name") String name,
            @RestForm("file") FileUpload fileUpload) {

        try {
            if (fileUpload == null) {
                throw new IllegalStateException("Fichier non reçu !");
            }

            File file = fileUpload.uploadedFile().toFile();

            // On passe le registry — la table sera créée si elle n'existe pas
            int inserted = ParquetImporter.loadParquet(file, name, registry);

            return Response.status(Response.Status.CREATED)
                    .entity(Map.of("inserted", inserted))
                    .build();

        } catch (Exception e) {
            logger.error("Erreur lors de l'import parquet", e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }


    @GET
    @Path("/{name}/select")
    public Response select(@PathParam("name") String name,
                           @QueryParam("columns") String columnsParam) {

        try {

            if (columnsParam == null || columnsParam.isBlank()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "columns parameter is required"))
                        .build();
            }

            List<String> columns = Arrays.asList(columnsParam.split(","));

            List<List<Object>> result = queryExecutionService.select(name, columns);

            return Response.ok(result).build();

        } catch (Exception e) {
            logger.error("Erreur lors du select sur la table", e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();

        }

    }

    @GET
    @Path("/{name}/query")
    public Response query(@PathParam("name") String name,
                          @QueryParam("q") String q) {
        try {
            if (q == null || q.isBlank()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "q parameter is required"))
                        .build();
            }
            List<List<Object>> result = queryExecutionService.selectQuery(name, q);
            return Response.ok(result).build();
        } catch (Exception e) {
            logger.error("Erreur lors de la query", e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage())).build();
        }
    }


    @POST
    @Path("/{name}/select-where")
    public Response selectWhere(@PathParam("name") String name,
                                Map<String, Object> body) {
        try {
            List<String> columns = (List<String>) body.get("columns");
            Map<String, Object> filterMap = (Map<String, Object>) body.get("filter");

            if (columns == null || columns.isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "columns are required"))
                        .build();
            }

            if (filterMap == null) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "filter is required"))
                        .build();
            }

            Filter filter = new Filter();
            filter.setColumn((String) filterMap.get("column"));
            filter.setOperator((String) filterMap.get("operator"));
            filter.setValue(filterMap.get("value"));

            List<List<Object>> result = queryExecutionService.selectWhere(name, columns, filter);
            logger.info("reussi");
            return Response.ok(result).build();

        } catch (Exception e) {
            logger.error("Erreur lors du selectWhere", e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }

    @GET
    @Path("/{name}/group-by")
    public Response groupBy(@PathParam("name") String name,
                            @QueryParam("column") String column) {
        try {
            if (column == null || column.isBlank()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "column parameter is required"))
                        .build();
            }

            List<List<Object>> result = queryExecutionService.groupByCount(name, column);
            return Response.ok(result).build();

        } catch (Exception e) {
            logger.error("Erreur lors du groupBy", e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }

    }

}
