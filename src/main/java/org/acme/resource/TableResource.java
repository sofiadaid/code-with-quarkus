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
            if (query == null || query.isBlank()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "q parameter is required"))
                        .build();
            }

            return Response.ok(queryService.selectQuery(name, query)).build();

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
    @Path("/{name}/create")
    public Response createEmpty(@PathParam("name") String name) {
        try {
            Table created = registry.createEmpty(name);
            return Response.status(Response.Status.CREATED)
                    .entity(Map.of("name", created.name, "message", "Table créée"))
                    .build();
        } catch (Exception e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage())).build();
        }
    }


}
