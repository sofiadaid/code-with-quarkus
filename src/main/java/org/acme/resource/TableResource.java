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
import org.jboss.resteasy.reactive.multipart.FileUpload;


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
            e.printStackTrace();
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
            List<List<Object>> result = registry.selectQuery(name, q);
            return Response.ok(result).build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage())).build();
        } catch (IllegalStateException e) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("error", e.getMessage())).build();
        }
    }

}
