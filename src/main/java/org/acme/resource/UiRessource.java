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
            <html lang="fr">
            <head>
                <meta charset="UTF-8"/>
                <title>Table Explorer</title>
                <style>
                    * { box-sizing: border-box; margin: 0; padding: 0; }
                    body { font-family: sans-serif; background: #f5f5f5; color: #1a1a1a; padding: 2rem; }
                    h1 { font-size: 20px; font-weight: 600; margin-bottom: 2rem; }
                    .card { background: white; border: 1px solid #e0e0e0; border-radius: 10px; padding: 1.25rem; margin-bottom: 1.5rem; }
                    .card h2 { font-size: 12px; font-weight: 600; color: #888; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 1rem; }
                    label { font-size: 13px; color: #555; display: block; margin-bottom: 4px; }
                    input[type=text], input[type=file], textarea, select {
                        width: 100%;
                        padding: 8px 10px;
                        font-size: 13px;
                        border: 1px solid #ddd;
                        border-radius: 6px;
                        background: white;
                        color: #1a1a1a;
                        margin-bottom: 10px;
                    }
                    textarea { resize: vertical; min-height: 60px; font-family: monospace; }
                    button {
                        padding: 8px 18px;
                        font-size: 13px;
                        cursor: pointer;
                        border-radius: 6px;
                        border: 1px solid #ccc;
                        background: #1a1a1a;
                        color: white;
                    }
                    button:hover { opacity: 0.85; }
                    .status {
                        display: none;
                        font-size: 13px;
                        margin-top: 10px;
                        padding: 8px 12px;
                        border-radius: 6px;
                    }
                    .ok  { background: #e8f5e9; color: #2e7d32; }
                    .err { background: #fdecea; color: #c62828; }
                    .info { background: #f0f0f0; color: #555; }
                    .chips { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 10px; }
                    .chip {
                        font-size: 12px;
                        padding: 3px 10px;
                        border-radius: 20px;
                        background: #f0f0f0;
                        color: #555;
                        cursor: pointer;
                        border: 1px solid #e0e0e0;
                    }
                    .chip:hover { border-color: #999; color: #1a1a1a; }
                    .meta { font-size: 12px; color: #888; margin-bottom: 8px; }
                    .overflow { overflow-x: auto; }
                    table { width: 100%; border-collapse: collapse; font-size: 13px; }
                    th {
                        text-align: left;
                        padding: 6px 10px;
                        font-size: 12px;
                        color: #888;
                        border-bottom: 1px solid #eee;
                    }
                    td {
                        padding: 6px 10px;
                        border-bottom: 1px solid #eee;
                    }
                    tr:last-child td { border-bottom: none; }
                    tr:hover td { background: #fafafa; }
                    .timer {
                        font-size: 13px;
                        font-variant-numeric: tabular-nums;
                        color: #555;
                        margin-top: 8px;
                        display: none;
                    }
                    .timer span {
                        font-weight: 600;
                        color: #1a1a1a;
                    }
                </style>
            </head>
            <body>
                <h1>Table Explorer</h1>

                <div class="card">
                    <h2>1 — Créer une table</h2>
                    <label>Nom de la table</label>
                    <input type="text" id="tableName" placeholder="ex: taxi" />
                    <p style="font-size:12px; color:#888; margin-bottom:10px;">
                        Ce nom sera utilisé pour la création, l'import, l'aperçu et les requêtes.
                    </p>
                    <button onclick="doCreate()">Créer</button>
                    <div id="createStatus"></div>
                </div>

                <div class="card">
                    <h2>2 — Importer un fichier Parquet</h2>
                    <p style="font-size:12px; color:#888; margin-bottom:10px;">
                        La table saisie en haut sera utilisée pour l'import.
                    </p>
                    <label>Fichier .parquet</label>
                    <input type="file" id="fileInput" accept=".parquet" />
                    <button onclick="doImport()">Importer</button>
                    <div id="importTimer" class="timer">⏱ Temps écoulé : <span id="importTimerValue">0.0s</span></div>
                    <div id="importStatus"></div>
                </div>

                <div class="card">
                    <h2>Aperçu du fichier parquet</h2>
                    <div style="display:flex; gap:10px; align-items:center; margin-bottom:10px;">
                        <label style="margin:0;">Lignes à afficher</label>
                        <select id="previewLimit" style="width:auto;">
                            <option value="10">10</option>
                            <option value="50">50</option>
                            <option value="100" selected>100</option>
                            <option value="500">500</option>
                        </select>
                    </div>
                    <button onclick="doPreview()">Afficher</button>
                    <div id="previewStatus"></div>
                    <div id="previewWrap" style="margin-top:16px; display:none;">
                        <div class="meta" id="previewMeta"></div>
                        <div class="overflow">
                            <table id="previewTable2"></table>
                        </div>
                    </div>
                </div>

                <div class="card">
                            <h2>Benchmark</h2>
                            <div style="display:flex; gap:10px;">
                                <button onclick="runBenchmark()">Benchmark synthétique</button>
                                <button onclick="runRealBenchmark()">Benchmark réel</button>
                            </div>
                            <div id="benchmarkStatus"></div>
                            <div id="benchmarkWrap" style="margin-top:16px; display:none;">
                                <div class="overflow">
                                    <table id="benchmarkTable"></table>
                                </div>
                            </div>
                            <div id="realBenchmarkResult" style="margin-top:16px;"></div>
                        </div>
                <!-- QUERY -->
                <div class="card">
                    <h2>Requête SELECT</h2>
                    <label>Exemples rapides</label>
                    <div class="chips">
                        <span class="chip" onclick="setQ('SELECT * LIMIT 10')">SELECT * LIMIT 10</span>
                        <span class="chip" onclick="setQ('SELECT VendorID, trip_distance, total_amount LIMIT 5')">SELECT colonnes</span>
                        <span class="chip" onclick="setQ('SELECT VendorID, total_amount WHERE total_amount > 50 ORDER BY total_amount DESC LIMIT 10')">WHERE + ORDER BY</span>
                        <span class="chip" onclick="setQ('SELECT trip_distance, fare_amount, tip_amount WHERE tip_amount > 20 ORDER BY tip_amount DESC LIMIT 10')">Pourboires élevés</span>
                        <span class="chip" onclick="setQ(&quot;SELECT VendorID, store_and_fwd_flag WHERE store_and_fwd_flag = 'Y' LIMIT 10&quot;)">WHERE string</span>
                    </div>
                    <label>Requête</label>
                    <textarea id="queryInput" placeholder="SELECT * LIMIT 10"></textarea>
                    <button onclick="doQuery()">Exécuter</button>
                    <div id="queryStatus"></div>
                    <div id="resultsWrap" style="margin-top:16px; display:none;">
                        <div class="meta" id="resultsMeta"></div>
                        <div class="overflow">
                            <table id="resultsTable"></table>
                        </div>
                    </div>
                </div>

                <script>
                    // --- Timer utilitaire ---
                    let importTimerInterval = null;

                    function startTimer(displayId, valueId) {
                        const display = document.getElementById(displayId);
                        const valueEl = document.getElementById(valueId);
                        const start = Date.now();
                        display.style.display = 'block';
                        valueEl.textContent = '0.0s';
                        importTimerInterval = setInterval(() => {
                            const elapsed = ((Date.now() - start) / 1000).toFixed(1);
                            valueEl.textContent = elapsed + 's';
                        }, 100);
                        return start;
                    }

                    function stopTimer(displayId, valueId, startTime) {
                        clearInterval(importTimerInterval);
                        importTimerInterval = null;
                        const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
                        document.getElementById(valueId).textContent = elapsed + 's';
                        return elapsed;
                    }

                    // --- Fin timer ---

                    function setQ(q) {
                        document.getElementById('queryInput').value = q;
                    }

                    function setStatus(id, msg, type) {
                        const el = document.getElementById(id);
                        if (!msg) {
                            el.className = '';
                            el.textContent = '';
                            el.style.display = 'none';
                            return;
                        }
                        el.className = 'status ' + type;
                        el.textContent = msg;
                        el.style.display = 'block';
                    }

                    function getTableName() {
                        return document.getElementById('tableName').value.trim();
                    }

                    async function doCreate() {
                        const name = getTableName();

                        if (!name) {
                            setStatus('createStatus', 'Remplissez le nom de la table.', 'err');
                            return;
                        }

                        setStatus('createStatus', 'Création en cours…', 'info');

                        try {
                            const r = await fetch('/api/tables', {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({ name: name, columns: [] })
                            });

                            const d = await r.json();

                            if (r.ok) {
                                setStatus('createStatus', `Table "${name}" créée avec succès.`, 'ok');
                            } else {
                                setStatus('createStatus', `Erreur : ${d.error}`, 'err');
                            }
                        } catch (e) {
                            setStatus('createStatus', 'Erreur réseau : ' + e.message, 'err');
                        }
                    }

                    async function doImport() {
                        const table = getTableName();
                        const file = document.getElementById('fileInput').files[0];

                        if (!table || !file) {
                            setStatus('importStatus', 'Remplissez le nom et choisissez un fichier.', 'err');
                            return;
                        }

                        setStatus('importStatus', 'Import en cours…', 'info');
                        const timerStart = startTimer('importTimer', 'importTimerValue');

                        const fd = new FormData();
                        fd.append('file', file);

                        try {
                            const r = await fetch(`/api/tables/${table}/import`, {
                                method: 'POST',
                                body: fd
                            });

                            const elapsed = stopTimer('importTimer', 'importTimerValue', timerStart);
                            const d = await r.json();

                            if (r.ok) {
                                setStatus('importStatus', `Succès ! ${d.inserted.toLocaleString()} lignes insérées en ${elapsed}s.`, 'ok');
                            } else {
                                setStatus('importStatus', `Erreur : ${d.error}`, 'err');
                            }
                        } catch (e) {
                            stopTimer('importTimer', 'importTimerValue', timerStart);
                            setStatus('importStatus', 'Erreur réseau : ' + e.message, 'err');
                        }
                    }

                    async function doPreview() {
                        const table = getTableName();
                        const limit = document.getElementById('previewLimit').value;
                        const file = document.getElementById('fileInput').files[0];

                        if (!table) {
                            setStatus('previewStatus', 'Veuillez entrer un nom de table.', 'err');
                            return;
                        }

                        if (!file) {
                            setStatus('previewStatus', 'Choisissez un fichier parquet.', 'err');
                            return;
                        }

                        setStatus('previewStatus', 'Chargement…', 'info');
                        document.getElementById('previewWrap').style.display = 'none';

                        try {
                            const fd = new FormData();
                            fd.append('file', file);

                            const rowsRes = await fetch(`/api/tables/preview?limit=${limit}`, {
                                method: 'POST',
                                body: fd
                            });

                            const data = await rowsRes.json();

                            if (!rowsRes.ok) {
                                setStatus('previewStatus', `Erreur : ${data.error}`, 'err');
                                return;
                            }

                            const colNames = data.columns || [];
                            const rows = data.rows || [];

                            renderPreview(colNames, rows, table);
                            setStatus('previewStatus', '', '');

                        } catch (e) {
                            setStatus('previewStatus', 'Erreur réseau : ' + e.message, 'err');
                        }
                    }

                    function renderPreview(colNames, rows, tableName) {
                        if (!rows.length) {
                            setStatus('previewStatus', 'Aucune ligne à afficher.', 'info');
                            return;
                        }

                        const tbl = document.getElementById('previewTable2');
                        tbl.innerHTML = '';

                        const thead = tbl.createTHead();
                        const hr = thead.insertRow();
                        colNames.forEach(c => {
                            const th = document.createElement('th');
                            th.textContent = c;
                            hr.appendChild(th);
                        });

                        const tbody = tbl.createTBody();
                        rows.forEach(row => {
                            const tr = tbody.insertRow();
                            row.forEach(val => {
                                const td = tr.insertCell();
                                td.textContent = val === null ? '—' : val;
                            });
                        });

                        document.getElementById('previewMeta').textContent =
                            `Aperçu pour "${tableName}" — ${rows.length.toLocaleString()} ligne(s) affichée(s)`;

                        document.getElementById('previewWrap').style.display = 'block';
                    }

                    async function doQuery() {
                        const table = getTableName();
                        const q = document.getElementById('queryInput').value.trim();

                        if (!table || !q) {
                            setStatus('queryStatus', 'Remplissez le nom de la table et la requête.', 'err');
                            return;
                        }

                        setStatus('queryStatus', 'Exécution…', 'info');
                        document.getElementById('resultsWrap').style.display = 'none';

                        try {
                            const schemaRes = await fetch(`/api/tables/${table}`);
                            const schema = await schemaRes.json();

                            if (!schemaRes.ok) {
                                setStatus('queryStatus', `Table introuvable : ${schema.error}`, 'err');
                                return;
                            }

                            const allColNames = (schema.columns || []).map(c => c.name);

                            const r = await fetch(`/api/tables/${table}/query?q=${encodeURIComponent(q)}`);
                            const d = await r.json();

                            if (!r.ok) {
                                setStatus('queryStatus', `Erreur : ${d.error}`, 'err');
                                return;
                            }

                            renderResults(d, q, allColNames);
                            setStatus('queryStatus', '', '');

                        } catch (e) {
                            setStatus('queryStatus', 'Erreur réseau : ' + e.message, 'err');
                        }
                    }

                    function renderResults(rows, q, allColNames) {
                        if (!rows.length) {
                            setStatus('queryStatus', 'Aucun résultat.', 'info');
                            return;
                        }

                        let afterSelect = q.trim().substring(6).trim();
                        ['WHERE', 'ORDER BY', 'LIMIT'].forEach(kw => {
                            const i = afterSelect.toUpperCase().indexOf(kw);
                            if (i !== -1) {
                                afterSelect = afterSelect.substring(0, i).trim();
                            }
                        });

                        const colNames = afterSelect === '*'
                            ? allColNames
                            : afterSelect.split(',').map(s => s.trim());

                        const tbl = document.getElementById('resultsTable');
                        tbl.innerHTML = '';

                        // Header
                        const thead = tbl.createTHead();
                        const hr = thead.insertRow();
                        colNames.forEach(c => {
                            const th = document.createElement('th');
                            th.textContent = c;
                            hr.appendChild(th);
                        });

                        // Body
                        const tbody = tbl.createTBody();
                        rows.forEach(row => {
                            const tr = tbody.insertRow();
                            row.forEach(val => {
                                const td = tr.insertCell();
                                td.textContent = val === null ? '—' : val;
                            });
                        });

                        document.getElementById('resultsMeta').textContent =
                            `${rows.length.toLocaleString()} ligne(s) retournée(s)`;
                        document.getElementById('resultsWrap').style.display = 'block';
                    }

                    function renderBenchmark(results) {
                        const table = document.getElementById('benchmarkTable');
                        table.innerHTML = '';

                        if (!results.length) {
                            setStatus('benchmarkStatus', 'Aucun résultat.', 'info');
                            return;
                        }

                        const headers = Object.keys(results[0]);

                        const thead = table.createTHead();
                        const hr = thead.insertRow();
                        headers.forEach(h => {
                            const th = document.createElement('th');
                            th.textContent = h;
                            hr.appendChild(th);
                        });

                        const tbody = table.createTBody();
                        results.forEach(row => {
                            const tr = tbody.insertRow();
                            headers.forEach(h => {
                                const td = tr.insertCell();
                                td.textContent = row[h];
                            });
                        });

                        document.getElementById('benchmarkWrap').style.display = 'block';
                    }

                    async function runBenchmark() {
                        setStatus('benchmarkStatus', 'Benchmark en cours...', 'info');
                        document.getElementById('benchmarkWrap').style.display = 'none';

                        try {
                            const res = await fetch('/api/benchmark/series');
                            const data = await res.json();

                            if (!res.ok) {
                                setStatus('benchmarkStatus', `Erreur : ${data.error}`, 'err');
                                return;
                            }

                            renderBenchmark(data);
                            setStatus('benchmarkStatus', 'Benchmark terminé', 'ok');

                        } catch (e) {
                            setStatus('benchmarkStatus', 'Erreur réseau : ' + e.message, 'err');
                        }
                    }

                    async function runRealBenchmark() {
                        const file = document.getElementById('fileInput').files[0];
                        const resultDiv = document.getElementById('realBenchmarkResult');

                        if (!file) {
                            setStatus('benchmarkStatus', 'Choisis un fichier parquet !', 'err');
                            return;
                        }

                        setStatus('benchmarkStatus', 'Benchmark réel en cours...', 'info');
                        resultDiv.innerHTML = '';

                        try {
                            const fd = new FormData();
                            fd.append('file', file);

                            const res = await fetch('/api/benchmark/load?repeat=3', {
                                method: 'POST',
                                body: fd
                            });

                            const raw = await res.text();
                            let data;
                            try {
                                data = JSON.parse(raw);
                            } catch {
                                setStatus('benchmarkStatus', `Réponse invalide du serveur : ${raw.substring(0, 200)}`, 'err');
                                return;
                            }

                            if (!res.ok) {
                                setStatus('benchmarkStatus', `Erreur : ${data.error ?? JSON.stringify(data)}`, 'err');
                                return;
                            }

                            resultDiv.innerHTML = `
                                <table>
                                    <thead>
                                        <tr><th>Metric</th><th>Value</th></tr>
                                    </thead>
                                    <tbody>
                                        <tr><td>Rows</td><td>${data.rows ?? '—'}</td></tr>
                                        <tr><td>Load (ms)</td><td>${data.loadMs ?? '—'}</td></tr>
                                        <tr><td>Select (ms)</td><td>${data.selectMs ?? '—'}</td></tr>
                                        ${data.whereMs != null ? `<tr><td>Where (ms)</td><td>${data.whereMs}</td></tr>` : ""}
                                        ${data.groupByMs != null ? `<tr><td>GroupBy (ms)</td><td>${data.groupByMs}</td></tr>` : ""}
                                    </tbody>
                                </table>
                            `;

                            setStatus('benchmarkStatus', 'Benchmark réel terminé ✔️', 'ok');

                        } catch (e) {
                            setStatus('benchmarkStatus', 'Erreur réseau : ' + e.message, 'err');
                        }
                    }
                </script>
            </body>
            </html>
        """;

        return Response.ok(html).build();
    }
}