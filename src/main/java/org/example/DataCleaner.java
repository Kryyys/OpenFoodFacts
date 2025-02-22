package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Arrays;

public class DataCleaner {
    public static Dataset<Row> cleanData(Dataset<Row> df) {
        // Affichage de la rentrée dans le CLEANER (pour essayer de voir où l'erreur "ne trouve pas product_name" apparaît)
        System.out.println("CLEANER");
        df.printSchema();

        // Suppression des lignes où product_name ou nutriments essentiels sont null
        df = df.filter("product_name IS NOT NULL AND energy_100g IS NOT NULL AND sugars_100g IS NOT NULL");

        // Remplacement des valeurs nulles par des valeurs par défaut
        df = df.na().fill("Inconnu", new String[]{"brands", "categories", "labels", "packaging", "countries"})
                .na().fill(0, new String[]{"sugars_100g", "fat_100g"});

        // Normalisation des noms de marques et des pays (conversion en minuscules et suppression espaces inutiles)
        df = df.withColumn("brands", functions.lower(functions.trim(df.col("brands"))));
        df = df.withColumn("countries", functions.lower(functions.trim(df.col("countries"))));

        // Suppression des doublons
        df = df.dropDuplicates();

        return df;
    }
}