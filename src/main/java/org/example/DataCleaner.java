package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.functions.*;
import java.util.Arrays;

public class DataCleaner {

    // Nettoyage des données
    public static Dataset<Row> cleanData(Dataset<Row> df) {
        // Supprimer les lignes avec des valeurs manquantes dans des colonnes essentielles
        df = df.filter(df.col("product_name").isNotNull())
                .filter(df.col("nutriments").isNotNull());

        // Remplir les valeurs manquantes pour les colonnes numériques avec 0.0 (Double)
        df = df.na().fill(0.0, new String[]{"sugars_100g", "fat_100g", "energy_100g"});

        // Supprimer les doublons
        df = df.dropDuplicates();

        return df;
    }
}