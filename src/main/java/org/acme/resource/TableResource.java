package org.acme.resource;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.acme.fic_csv.CsvImporter;
import org.acme.model.Table;
import org.acme.service.TableRegistry;

import java.util.Map;

import java.io.InputStream;
import java.io.File;

import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import java.util.List;
import java.util.Arrays;
import java.util.Map;

import org.acme.importer.ParquetImporter;
import org.jboss.resteasy.reactive.RestForm;


@Path("/api/tables")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableResource {

    @Inject
    TableRegistry registry;

    @POST
    public Response create(Table req) {
        try {
            Table created = registry.create(req);
            return Response.status(Response.Status.CREATED).entity(created).build();
        } catch (Exception e) {
            e.printStackTrace();
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
            e.printStackTrace();
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
            e.printStackTrace();
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
            @RestForm("file") InputStream fileStream) {

        try {
            if (fileStream == null) {
                throw new IllegalStateException("Fichier non reçu !");
            }

            Table table = registry.get(name)
                    .orElseThrow(() -> new IllegalStateException("Table not found: " + name));

            File tempFile = File.createTempFile("import_", ".parquet");
            Files.copy(fileStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            int inserted = ParquetImporter.loadParquet(tempFile, table);

            tempFile.delete();

            return Response.status(Response.Status.CREATED)
                    .entity(Map.of("inserted", inserted))
                    .build();

        } catch (Exception e) {
            e.printStackTrace(); // 🔥 IMPORTANT pour debug
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

            List<List<Object>> result = registry.select(name, columns);

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

}
