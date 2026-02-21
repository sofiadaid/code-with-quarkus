package org.acme.resource;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.acme.fic_csv.CsvImporter;
import org.acme.model.Table;
import org.acme.service.TableRegistry;

import java.util.Map;

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
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        } catch (IllegalStateException e) {
            return Response.status(Response.Status.CONFLICT)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }

    @GET
    public Response list() {
        return Response.ok(registry.list()).build();
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

    @DELETE
    @Path("/{name}")
    public Response drop(@PathParam("name") String name) {
        boolean deleted = registry.drop(name);
        if (!deleted) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("error", "Table not found: " + name))
                    .build();
        }
        return Response.noContent().build(); // 204
    }

    @POST
    @Path("/{name}/rows")
    public Response insertRows(@PathParam("name") String name, java.util.List<java.util.List<Object>> inputRows) {
        try {
            int inserted = registry.insertRows(name, inputRows);
            return Response.status(Response.Status.CREATED)
                    .entity(java.util.Map.of("inserted", inserted))
                    .build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(java.util.Map.of("error", e.getMessage()))
                    .build();
        } catch (IllegalStateException e) {
            return Response.status(Response.Status.NOT_FOUND)
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
        } catch (IllegalStateException e) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(java.util.Map.of("error", e.getMessage()))
                    .build();
        }
    }

    @POST
    @Path("/{name}/import")
    public Response importCsv(@PathParam("name") String name,
                              java.util.Map<String, String> body) {

        try {

            String path = body.get("path");

            int inserted = CsvImporter.importCsv(
                    name,
                    path,
                    registry
            );

            return Response.status(Response.Status.CREATED)
                    .entity(java.util.Map.of("inserted", inserted))
                    .build();

        } catch (Exception e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(java.util.Map.of("error", e.getMessage()))
                    .build();
        }
    }

}
