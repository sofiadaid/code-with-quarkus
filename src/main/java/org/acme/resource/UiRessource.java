package org.acme.resource;



import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;



@Path("/ui")

public class UiRessource {



    @GET

    @Produces(MediaType.TEXT_HTML)

    public Response ui() {

        String html = """

            <!DOCTYPE html>

            <html>

            <body>

                <h2>Importer un fichier Parquet</h2>

                

                <input type="text" id="tableName" placeholder="Nom de la table" />

                <br/><br/>

                <input type="file" id="fileInput" accept=".parquet" />

                <br/><br/>

                <button onclick="uploadFile()">Importer</button>

                

                <p id="result"></p>

                

                <script>

                        async function uploadFile() {
                                    const table = document.getElementById("tableName").value.trim();
                                    const file = document.getElementById("fileInput").files[0];
                
                                    if (!table || !file) {
                                        document.getElementById("result").textContent = "Remplis tous les champs.";
                                        return;
                                    }
                
                                    const formData = new FormData();
                                    formData.append("file", file);
                
                                    try {
                                        const response = await fetch(`/api/tables/${table}/import`, {
                                            method: "POST",
                                            body: formData
                                        });
                
                                        const text = await response.text(); // ⚠️ pas json()
                
                                        console.log("STATUS:", response.status);
                                        console.log("RESPONSE:", text);
                
                                        document.getElementById("result").textContent =
                                            "Status: " + response.status + " -> " + text;
                
                                    } catch (e) {
                                        console.error("ERREUR:", e);
                                        document.getElementById("result").textContent = e;
                                    }
                                }

                        </script>

            </body>

            </html>

        """;

        return Response.ok(html).build();

    }

}