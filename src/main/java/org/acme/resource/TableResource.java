package org.acme.resource;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.acme.importer.ParquetImporter;
import org.acme.model.Filter;
import org.acme.model.Table;
import org.acme.service.QueryExecutionService;
import org.acme.service.TableRegistry;
import org.jboss.resteasy.reactive.RestForm;
import org.jboss.resteasy.reactive.multipart.FileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

@Path("/api/tables")
@Produces(MediaType.APPLICATION_JSON)
public class TableResource {

    private static final Logger logger = LoggerFactory.getLogger(TableResource.class);

    @Inject
    TableRegistry registry;

    @Inject
    QueryExecutionService queryService;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
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
                .map(table -> Response.ok(table).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity(Map.of("error", "Table not found: " + name))
                        .build());
    }

    @POST
    @Path("/{name}/rows")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response insertRows(@PathParam("name") String name, List<List<Object>> inputRows) {
        try {
            int inserted = registry.insertRows(name, inputRows);
            return Response.status(Response.Status.CREATED)
                    .entity(Map.of("inserted", inserted))
                    .build();
        } catch (Exception e) {
            logger.error("Erreur insertion lignes", e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
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
            logger.error("Erreur lecture lignes", e);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }

    @POST
    @Path("/{name}/import")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response importParquet(@PathParam("name") String name,
                                  @RestForm("file") FileUpload fileUpload) {
        try {
            if (fileUpload == null) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Missing file"))
                        .build();
            }

            File file = fileUpload.uploadedFile().toFile();
            int inserted = ParquetImporter.loadParquet(file, name, registry);

            return Response.status(Response.Status.CREATED)
                    .entity(Map.of("inserted", inserted))
                    .build();

        } catch (Exception e) {
            logger.error("Erreur import parquet", e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }

    @GET
    @Path("/{name}/query")
    public Response query(@PathParam("name") String name,
                          @QueryParam("q") String query) {
        try {
            System.out.println("========== QUERY DEBUG ==========");
            System.out.println("TABLE = " + name);
            System.out.println("RAW QUERY = " + query);

            if (query == null || query.isBlank()) {
                System.out.println("ERROR: q parameter is missing");
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "q parameter is required"))
                        .build();
            }

            List<List<Object>> result = queryService.selectQuery(name, query);

            System.out.println("RESULT SIZE = " + result.size());
            if (!result.isEmpty()) {
                System.out.println("FIRST ROW = " + result.get(0));
            }
            System.out.println("================================");

            return Response.ok(result).build();

        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();

        } catch (IllegalStateException e) {
            e.printStackTrace();
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("error", e.getMessage()))
                    .build();

        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }

    @POST
    @Path("/{name}/select")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response select(@PathParam("name") String name, List<String> selectedColumns) {
        try {
            if (selectedColumns == null || selectedColumns.isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "At least one column is required"))
                        .build();
            }

            return Response.ok(queryService.select(name, selectedColumns)).build();

        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();

        } catch (IllegalStateException e) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }

    @POST
    @Path("/{name}/select-where")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response selectWhere(@PathParam("name") String name, SelectWhereRequest request) {
        try {
            if (request == null || request.columns == null || request.columns.isEmpty() || request.filter == null) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "columns and filter are required"))
                        .build();
            }

            return Response.ok(queryService.selectWhere(name, request.columns, request.filter)).build();

        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();

        } catch (IllegalStateException e) {
            return Response.status(Response.Status.NOT_FOUND)
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

            return Response.ok(queryService.groupByCount(name, column)).build();

        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();

        } catch (IllegalStateException e) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }

    @GET
    @Path("/{name}/aggregate")
    public Response aggregate(@PathParam("name") String name,
                              @QueryParam("function") String function,
                              @QueryParam("column") String column) {
        try {
            if (function == null || function.isBlank()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "function parameter is required"))
                        .build();
            }

            Object result;

            switch (function.trim().toUpperCase()) {
                case "COUNT":
                    result = queryService.count(name, column);
                    break;
                case "MIN":
                    result = queryService.min(name, column);
                    break;
                case "MAX":
                    result = queryService.max(name, column);
                    break;
                case "SUM":
                    result = queryService.sum(name, column);
                    break;
                case "AVG":
                case "MEAN":
                    result = queryService.avg(name, column);
                    break;
                default:
                    return Response.status(Response.Status.BAD_REQUEST)
                            .entity(Map.of("error", "Unsupported aggregate function: " + function))
                            .build();
            }

            return Response.ok(Map.of(
                    "table", name,
                    "function", function.toUpperCase(),
                    "column", column,
                    "result", result
            )).build();

        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();

        } catch (IllegalStateException e) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }

    @GET
    @Path("/{name}/group-by/aggregate")
    public Response groupByAggregate(@PathParam("name") String name,
                                     @QueryParam("groupBy") String groupBy,
                                     @QueryParam("function") String function,
                                     @QueryParam("column") String column) {
        try {
            if (groupBy == null || groupBy.isBlank()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "groupBy parameter is required"))
                        .build();
            }

            if (function == null || function.isBlank()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "function parameter is required"))
                        .build();
            }

            List<List<Object>> result;

            switch (function.trim().toUpperCase()) {
                case "COUNT":
                    result = queryService.groupByCount(name, groupBy);
                    break;
                case "MIN":
                    result = queryService.groupByMin(name, groupBy, column);
                    break;
                case "MAX":
                    result = queryService.groupByMax(name, groupBy, column);
                    break;
                case "SUM":
                    result = queryService.groupBySum(name, groupBy, column);
                    break;
                case "AVG":
                case "MEAN":
                    result = queryService.groupByAvg(name, groupBy, column);
                    break;
                default:
                    return Response.status(Response.Status.BAD_REQUEST)
                            .entity(Map.of("error", "Unsupported aggregate function: " + function))
                            .build();
            }

            return Response.ok(result).build();

        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();

        } catch (IllegalStateException e) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }

    public static class SelectWhereRequest {
        public List<String> columns;
        public Filter filter;
    }

    @POST
    @Path("/preview")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response previewParquet(@RestForm("file") FileUpload fileUpload,
                                   @QueryParam("limit") @DefaultValue("10") int limit) {
        try {
            if (fileUpload == null) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Missing file"))
                        .build();
            }

            File file = fileUpload.uploadedFile().toFile();

            List<Object[]> preview = ParquetImporter.previewParquet(file, limit);

            return Response.ok(preview).build();

        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }
}