package org.acme.service;

import jakarta.enterprise.context.ApplicationScoped;
import org.acme.model.Table;

/**
 * Service de persistence disque — à implémenter.
 *
 * Objectif : sauvegarder les tables sur disque (./data/{tableName}.bin)
 * pour ne pas perdre les données au redémarrage de Quarkus.
 *
 * TODO:
 *   - save(Table table)  → sérialisation binaire
 *   - load(String name)  → désérialisation au démarrage
 */
@ApplicationScoped
public class StorageService {

    public void save(Table table) {
        // TODO : sérialisation binaire sur disque
    }

    public Table load(String tableName) {
        // TODO : désérialisation depuis disque
        return null;
    }
}
